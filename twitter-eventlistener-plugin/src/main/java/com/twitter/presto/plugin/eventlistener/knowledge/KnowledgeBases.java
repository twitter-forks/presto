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

import java.util.List;
import java.util.Optional;

public class KnowledgeBases
{
    private final List<KnowledgeBase> knowledge;

    @JsonCreator
    public KnowledgeBases(
            @JsonProperty("knowledge") List<KnowledgeBase> knowledge)
    {
        this.knowledge = knowledge;
    }

    @JsonProperty
    public List<KnowledgeBase> getKnowledge()
    {
        return knowledge;
    }

    public Optional<String> getTreatment(String failureMessage)
    {
        return knowledge.stream()
                .map(knowledge -> knowledge.match(failureMessage))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }
}
