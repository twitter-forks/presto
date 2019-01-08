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
package com.twitter.presto.plugin.eventlistener.slack;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * In addition to match a present string, this class defines a pattern to match absent string.
 * Example:
 * We have `principal` instantiated from its json definition:
 *
 *     "principal": {
 *         "isAbsent": true,
 *         "regex": "user.*"
 *     }
 *
 * The `matches(Optional<String> value)` will return `true` when either of conditions matches.
 *
 * principal.matches(Optional.empty()) -> true
 * principal.matches(Optional.of(""))  -> false
 * principal.matches(Optional.of("user"))  -> true
 * principal.matches(Optional.of("user@example.top")) -> true
 *
 * The effective default value for "isAbsent" is `true`.
 * The effective default value for "regex" is `.*`.
 *
 */
public class AbsentOrPattern
{
    private final Optional<Boolean> isAbsent;
    private final Optional<Pattern> regex;

    @JsonCreator
    public AbsentOrPattern(
            @JsonProperty("isAbsent") Optional<Boolean> isAbsent,
            @JsonProperty("regex") Optional<Pattern> regex)
    {
        this.isAbsent = isAbsent;
        this.regex = regex;
    }

    public boolean matches(Optional<String> value)
    {
        return value.map(v -> regex.map(r -> r.matcher(v).matches()).orElse(true))
                .orElse(isAbsent.orElse(true));
    }
}
