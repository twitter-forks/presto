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
import io.airlift.units.Duration;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class SlackNotificationTemplate
{
    private static final Pattern ANY = Pattern.compile(".*");
    private final String text;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> eventRegex;
    private final Optional<Pattern> stateRegex;
    private final Optional<AbsentOrPattern> principal;
    private final Optional<Duration> minWallTime;
    private final Optional<Pattern> errorTypeRegex;

    @JsonCreator
    public SlackNotificationTemplate(
            @JsonProperty("text") String text,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("event") Optional<Pattern> eventRegex,
            @JsonProperty("state") Optional<Pattern> stateRegex,
            @JsonProperty("principal") Optional<AbsentOrPattern> principal,
            @JsonProperty("minWallTime") Optional<Duration> minWallTime,
            @JsonProperty("errorType") Optional<Pattern> errorTypeRegex)
    {
        this.text = text;
        this.userRegex = userRegex;
        this.eventRegex = eventRegex;
        this.stateRegex = stateRegex;
        this.principal = principal;
        this.minWallTime = minWallTime;
        this.errorTypeRegex = errorTypeRegex;
    }

    public Optional<String> match(String user, String event, String state, Map<String, Optional<String>> fields)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                eventRegex.map(regex -> regex.matcher(event).matches()).orElse(true) &&
                stateRegex.map(regex -> regex.matcher(state).matches()).orElse(true) &&
                principal.map(pattern -> checkOptionalFields(pattern, "principal", fields)).orElse(true) &&
                errorTypeRegex.map(regex -> checkOptionalFields(regex, "error_type", fields)).orElse(true) &&
                minWallTime.map(regex -> checkOptionalMinimumDuration(regex, "wall_time", fields)).orElse(true) &&
                (!text.contains(SlackBot.FAILURE_MESSAGE) || checkOptionalFields(ANY, "failure_message", fields)) &&
                (!text.contains(SlackBot.FAILURE_TREATMENT) || checkOptionalFields(ANY, "failure_treatment", fields))) {
            return Optional.of(text);
        }
        return Optional.empty();
    }

    private boolean checkOptionalFields(AbsentOrPattern pattern, String key, Map<String, Optional<String>> fields)
    {
        Optional<String> field = fields.getOrDefault(key, Optional.empty());
        return pattern.matches(field);
    }

    private boolean checkOptionalFields(Pattern regex, String key, Map<String, Optional<String>> fields)
    {
        Optional<String> field = fields.getOrDefault(key, Optional.empty());
        return field.isPresent() && regex.matcher(field.get()).matches();
    }

    private boolean checkOptionalMinimumDuration(Duration minDuration, String key, Map<String, Optional<String>> fields)
    {
        Optional<String> field = fields.getOrDefault(key, Optional.empty());
        return field.isPresent() && minDuration.compareTo(Duration.valueOf(field.get())) <= 0;
    }
}
