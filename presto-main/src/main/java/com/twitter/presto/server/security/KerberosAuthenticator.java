/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.presto.server.security;

import com.facebook.presto.server.security.AuthenticationException;
import com.facebook.presto.server.security.Authenticator;
import com.facebook.presto.server.security.KerberosConfig;
import com.sun.security.auth.module.Krb5LoginModule;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.joda.time.DateTime;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.util.Objects.requireNonNull;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.ietf.jgss.GSSCredential.ACCEPT_ONLY;
import static org.ietf.jgss.GSSCredential.INDEFINITE_LIFETIME;

public class KerberosAuthenticator
        implements Authenticator
{
    private static final Logger LOG = Logger.get(KerberosAuthenticator.class);
    private static final Duration MIN_CREDENTIAL_LIFE_TIME = new Duration(60, TimeUnit.SECONDS);
    private static final Duration DEFAULT_LIFT_TIME = new Duration(1, TimeUnit.HOURS);

    private static final String NEGOTIATE_SCHEME = "Negotiate";

    private final GSSManager gssManager = GSSManager.getInstance();
    private final GSSName gssName;
    private final File keyTab;
    private final String servicePrincipal;
    private Session serverSession;

    @Inject
    public KerberosAuthenticator(KerberosConfig config)
    {
        System.setProperty("java.security.krb5.conf", config.getKerberosConfig().getAbsolutePath());

        try {
            boolean isCompleteServicePrinciple = config.getServiceName().contains("@");
            String hostname = InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
            servicePrincipal = isCompleteServicePrinciple ? config.getServiceName() : config.getServiceName() + "/" + hostname;
            gssName = isCompleteServicePrinciple ? gssManager.createName(config.getServiceName(), GSSName.NT_USER_NAME) : gssManager.createName(config.getServiceName() + "@" + hostname, GSSName.NT_HOSTBASED_SERVICE);
            keyTab = config.getKeytab();
        }
        catch (UnknownHostException | GSSException e) {
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            getSession(false).getLoginContext().logout();
        }
        catch (LoginException | GSSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Principal authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        String header = request.getHeader(AUTHORIZATION);

        String requestSpnegoToken = null;

        if (header != null) {
            String[] parts = header.split("\\s+");
            if (parts.length == 2 && parts[0].equals(NEGOTIATE_SCHEME)) {
                try {
                    requestSpnegoToken = parts[1];
                    Optional<Principal> principal = authenticate(parts[1]);
                    if (principal.isPresent()) {
                        return principal.get();
                    }
                }
                catch (RuntimeException e) {
                    throw new RuntimeException("Authentication error for token: " + parts[1], e);
                }
            }
        }

        if (requestSpnegoToken != null) {
            throw new AuthenticationException("Authentication failed for token: " + requestSpnegoToken, NEGOTIATE_SCHEME);
        }

        throw new AuthenticationException(null, NEGOTIATE_SCHEME);
    }

    private Optional<Principal> authenticate(String token)
    {
        try {
            Session session = getSession(false);
            LOG.debug("session remaining lift time: %d seconds", session.getRemainingLifetime());
            GSSContext context = doAs(session.getLoginContext().getSubject(), () -> gssManager.createContext(session.getServerCredential()));

            try {
                byte[] inputToken = Base64.getDecoder().decode(token);
                context.acceptSecContext(inputToken, 0, inputToken.length);

                // We can't hold on to the GSS context because HTTP is stateless, so fail
                // if it can't be set up in a single challenge-response cycle
                if (context.isEstablished()) {
                    return Optional.of(new KerberosPrincipal(context.getSrcName().toString()));
                }
                LOG.debug("Failed to establish GSS context for token %s", token);
            }
            catch (GSSException e) {
                // ignore and fail the authentication
                LOG.debug(e, "Authentication failed for token %s", token);
                // try force session refresh for certain conditions
                if (session.getAge() > MIN_CREDENTIAL_LIFE_TIME.getValue(TimeUnit.SECONDS)
                        && e.getMessage().contains("Cannot find key of appropriate type")) {
                    getSession(true);
                }
            }
            finally {
                try {
                    context.dispose();
                }
                catch (GSSException e) {
                    // ignore
                }
            }
        }
        catch (LoginException | GSSException e) {
            //ignore
            LOG.debug(e, "Authenticator failed to get session");
        }

        return Optional.empty();
    }

    private synchronized Session getSession(boolean isForceRefresh)
            throws LoginException, GSSException
    {
        if (isForceRefresh || serverSession == null || serverSession.getRemainingLifetime() < MIN_CREDENTIAL_LIFE_TIME.getValue(TimeUnit.SECONDS)) {
            // TODO: do we need to call logout() on the LoginContext?

            LoginContext loginContext = new LoginContext("", null, null, new Configuration()
            {
                @Override
                public AppConfigurationEntry[] getAppConfigurationEntry(String name)
                {
                    Map<String, String> options = new HashMap<>();
                    options.put("refreshKrb5Config", "true");
                    options.put("doNotPrompt", "true");
                    if (LOG.isDebugEnabled()) {
                        options.put("debug", "true");
                    }
                    if (keyTab != null) {
                        options.put("keyTab", keyTab.getAbsolutePath());
                    }
                    options.put("isInitiator", "false");
                    options.put("useKeyTab", "true");
                    options.put("principal", servicePrincipal);
                    options.put("storeKey", "true");

                    return new AppConfigurationEntry[] {new AppConfigurationEntry(Krb5LoginModule.class.getName(), REQUIRED, options)};
                }
            });
            loginContext.login();

            GSSCredential serverCredential = doAs(loginContext.getSubject(), () -> gssManager.createCredential(
                    gssName,
                    INDEFINITE_LIFETIME,
                    new Oid[] {
                            new Oid("1.2.840.113554.1.2.2"), // kerberos 5
                            new Oid("1.3.6.1.5.5.2") // spnego
                    },
                    ACCEPT_ONLY));

            serverSession = new Session(loginContext, serverCredential, (int) DEFAULT_LIFT_TIME.getValue(TimeUnit.SECONDS));
        }

        return serverSession;
    }

    private interface GssSupplier<T>
    {
        T get()
                throws GSSException;
    }

    private static <T> T doAs(Subject subject, GssSupplier<T> action)
    {
        return Subject.doAs(subject, (PrivilegedAction<T>) () -> {
            try {
                return action.get();
            }
            catch (GSSException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static class Session
    {
        private final LoginContext loginContext;
        private final GSSCredential serverCredential;
        private final DateTime createdTime;
        private final DateTime expiredTime;

        public Session(LoginContext loginContext, GSSCredential serverCredential, int lifetime)
        {
            requireNonNull(loginContext, "loginContext is null");
            requireNonNull(serverCredential, "gssCredential is null");

            this.loginContext = loginContext;
            this.serverCredential = serverCredential;
            this.createdTime = DateTime.now();
            this.expiredTime = createdTime.plusSeconds(lifetime);
        }

        public LoginContext getLoginContext()
        {
            return loginContext;
        }

        public GSSCredential getServerCredential()
        {
            return serverCredential;
        }

        public int getAge()
        {
            return (int) Duration.succinctDuration(DateTime.now().getMillis() - createdTime.getMillis(), TimeUnit.MILLISECONDS).getValue(TimeUnit.SECONDS);
        }

        public int getRemainingLifetime()
                throws GSSException
        {
            return Math.min(serverCredential.getRemainingLifetime(),
                    (int) Duration.succinctDuration(expiredTime.getMillis() - DateTime.now().getMillis(), TimeUnit.MILLISECONDS).getValue(TimeUnit.SECONDS));
        }
    }
}
