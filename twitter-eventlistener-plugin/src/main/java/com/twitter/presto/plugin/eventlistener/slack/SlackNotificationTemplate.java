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

public class SlackNotificationTemplate
{
    private final String text;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> principalRegex;
    private final Optional<Pattern> eventRegex;
    private final Optional<Pattern> stateRegex;

    @JsonCreator
    public SlackNotificationTemplate(
            @JsonProperty("text") String text,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("principal") Optional<Pattern> principalRegex,
            @JsonProperty("event") Optional<Pattern> eventRegex,
            @JsonProperty("state") Optional<Pattern> stateRegex)
    {
        this.text = text;
        this.userRegex = userRegex;
        this.principalRegex = principalRegex;
        this.eventRegex = eventRegex;
        this.stateRegex = stateRegex;
    }

    public Optional<String> match(String user, String principal, String event, String state)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                principalRegex.map(regex -> regex.matcher(principal).matches()).orElse(true) &&
                eventRegex.map(regex -> regex.matcher(event).matches()).orElse(true) &&
                stateRegex.map(regex -> regex.matcher(state).matches()).orElse(true)) {
            return Optional.of(text);
        }
        return Optional.empty();
    }
}
