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
package com.twitter.presto.plugin.eventlistener.knowledge;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

public class KnowledgeBase
{
    private final Pattern failureMessageRegex;
    private final String treatment;

    @JsonCreator
    public KnowledgeBase(
            @JsonProperty("failure_message") Pattern failureMessageRegex,
            @JsonProperty("treatment") String treatment)
    {
        this.failureMessageRegex = failureMessageRegex;
        this.treatment = treatment;
    }

    @JsonProperty
    public Pattern getFailureMessageRegex()
    {
        return failureMessageRegex;
    }

    @JsonProperty
    public String getTreatment()
    {
        return treatment;
    }

    public Optional<String> match(String failureMessage)
    {
        if (failureMessageRegex.matcher(failureMessage).matches()) {
            return Optional.of(treatment);
        }
        return Optional.empty();
    }
}
