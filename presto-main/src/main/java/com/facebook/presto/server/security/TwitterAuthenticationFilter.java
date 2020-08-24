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
package com.facebook.presto.server.security;

import com.facebook.airlift.http.server.AuthenticationException;
import com.facebook.airlift.http.server.Authenticator;
import com.facebook.airlift.log.Logger;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class TwitterAuthenticationFilter
        implements Filter
{
    private static final Logger LOG = Logger.get(TwitterAuthenticationFilter.class);

    private final List<Authenticator> authenticators;

    private final String httpAuthenticationPathRegex;
    private final boolean allowByPass;
    private final String statementSourceByPassRegex;

    @Inject
    public TwitterAuthenticationFilter(List<Authenticator> authenticators, SecurityConfig securityConfig)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        this.httpAuthenticationPathRegex = requireNonNull(securityConfig.getHttpAuthenticationPathRegex(), "httpAuthenticationPathRegex is null");
        this.allowByPass = securityConfig.getAllowByPass();
        this.statementSourceByPassRegex = securityConfig.getStatementSourceByPassRegex();
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        // skip authentication if (not configured) or (non-secure and not match
        if (authenticators.isEmpty() || (!request.isSecure() && !request.getPathInfo().matches(httpAuthenticationPathRegex))) {
            nextFilter.doFilter(request, response);
            return;
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            Principal principal;
            try {
                principal = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                if (e.getMessage() != null) {
                    messages.add(e.getMessage());
                }
                e.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                continue;
            }

            // authentication succeeded
            nextFilter.doFilter(withPrincipal(request, principal), response);
            return;
        }

        //authentication bypassed
        if (allowByPass) {
            //TODO: remove this field once we enforced authentication 100%
            if (request.getMethod().equals("GET") && request.getPathInfo().startsWith("/v1/statement") ||
                    (statementSourceByPassRegex != null
                            && request.getHeader(PRESTO_SOURCE) != null
                            && request.getHeader(PRESTO_SOURCE).matches(statementSourceByPassRegex))) {
                nextFilter.doFilter(request, response);
                LOG.debug("Authentication by passed from source: %s user: %s", request.getHeader(PRESTO_SOURCE), request.getHeader(PRESTO_USER));
                return;
            }
        }

        // authentication failed
        skipRequestBody(request);

        for (String value : authenticateHeaders) {
            response.addHeader(WWW_AUTHENTICATE, value);
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }
        LOG.debug("Auth failed %s %s %s %s", request.getHeader(PRESTO_SOURCE), request.getHeader(PRESTO_USER), request.getPathInfo(), request.getQueryString());
        response.sendError(SC_UNAUTHORIZED, Joiner.on(" | ").join(messages));
    }

    private static ServletRequest withPrincipal(HttpServletRequest request, Principal principal)
    {
        requireNonNull(principal, "principal is null");
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return principal;
            }
        };
    }

    private static void skipRequestBody(HttpServletRequest request)
            throws IOException
    {
        // If we send the challenge without consuming the body of the request,
        // the server will close the connection after sending the response.
        // The client may interpret this as a failed request and not resend the
        // request with the authentication header. We can avoid this behavior
        // in the client by reading and discarding the entire body of the
        // unauthenticated request before sending the response.
        try (InputStream inputStream = request.getInputStream()) {
            copy(inputStream, nullOutputStream());
        }
    }
}
