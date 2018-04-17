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
package com.facebook.presto.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class PrincipalUserMatchRule
{
    private final boolean allow;
    private final Pattern principalRegex;
    private final Optional<String> userRegex;

    @JsonCreator
    public PrincipalUserMatchRule(
            @JsonProperty("allow") boolean allow,
            @JsonProperty("principal") Pattern principalRegex,
            @JsonProperty("user") Optional<String> userRegex)
    {
        this.allow = allow;
        this.principalRegex = requireNonNull(principalRegex, "principalRegex is null");
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
    }

    public Optional<Boolean> match(String principal, String user)
    {
        Matcher matcher = principalRegex.matcher(principal);
        if (matcher.matches()) {
            if (userRegex.isPresent()) {
                String userPattern = matcher.replaceAll(userRegex.get());
                if (Pattern.compile(userPattern).matcher(user).matches()) {
                    return Optional.of(allow);
                }
            }
            else {
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    String extractedUsername = matcher.group(i);
                    if (user.equals(extractedUsername)) {
                        return Optional.of(allow);
                    }
                }
            }
        }

        return Optional.empty();
    }
}
