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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.KERBEROS;
import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.LDAP;

public class TestSecurityConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(SecurityConfig.class)
                .setAuthenticationTypes("")
                .setHttpAuthenticationPathRegex("^\b$"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("http-server.authentication.type", "KERBEROS,LDAP")
                .put("http-server.http.authentication.path.regex", "^/v1/statement")
                .build();

        SecurityConfig expected = new SecurityConfig()
                .setAuthenticationTypes(ImmutableSet.of(KERBEROS, LDAP))
                .setHttpAuthenticationPathRegex("^/v1/statement");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
