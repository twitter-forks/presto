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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;

@DefunctConfig("http.server.authentication.enabled")
public class SecurityConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<AuthenticationType> authenticationTypes = ImmutableList.of();
    private String httpAuthenticationPathRegex = "^\b$";

    private boolean allowByPass;
    private String statementSourceByPassRegex = "^\\b$";

    public enum AuthenticationType
    {
        CERTIFICATE,
        KERBEROS,
        PASSWORD,
        JWT
    }

    @NotNull
    public List<AuthenticationType> getAuthenticationTypes()
    {
        return authenticationTypes;
    }

    public SecurityConfig setAuthenticationTypes(List<AuthenticationType> authenticationTypes)
    {
        this.authenticationTypes = ImmutableList.copyOf(authenticationTypes);
        return this;
    }

    @NotNull
    public boolean getAllowByPass()
    {
        return allowByPass;
    }

    public String getStatementSourceByPassRegex()
    {
        return statementSourceByPassRegex;
    }

    @Config("http-server.authentication.type")
    @ConfigDescription("Authentication types (supported types: CERTIFICATE, KERBEROS, PASSWORD, JWT)")
    public SecurityConfig setAuthenticationTypes(String types)
    {
        if (types == null) {
            authenticationTypes = null;
            return this;
        }

        authenticationTypes = stream(SPLITTER.split(types))
                .map(AuthenticationType::valueOf)
                .collect(toImmutableList());
        return this;
    }

    @NotNull
    public String getHttpAuthenticationPathRegex()
    {
        return httpAuthenticationPathRegex;
    }

    @Config("http-server.http.authentication.path.regex")
    @ConfigDescription("Regex of path that needs to be authenticated for non-secured http request")
    public SecurityConfig setHttpAuthenticationPathRegex(String regex)
    {
        httpAuthenticationPathRegex = regex;
        return this;
    }

    @Config("http-server.authentication.allow-by-pass")
    @ConfigDescription("Allow authentication by pass")
    public SecurityConfig setAllowByPass(boolean allowByPass)
    {
        this.allowByPass = allowByPass;
        return this;
    }

    @Config("http-server.statement.source.allow-by-pass-regex")
    @ConfigDescription("Regex of the statement source that allows bypass authentication")
    public SecurityConfig setStatementSourceByPassRegex(String regex)
    {
        this.statementSourceByPassRegex = regex;
        return this;
    }
}
