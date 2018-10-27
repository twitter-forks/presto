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

import java.util.List;
import java.util.Optional;

public class SlackImHistoryResponse
        extends SlackResponse
{
    private final Optional<String> latest;
    private final Optional<List<SlackMessage>> messages;
    private final Optional<Boolean> hasMore;

    @JsonCreator
    public SlackImHistoryResponse(
            @JsonProperty("ok") boolean ok,
            @JsonProperty("latest") Optional<String> latest,
            @JsonProperty("messages") Optional<List<SlackMessage>> messages,
            @JsonProperty("has_more") Optional<Boolean> hasMore,
            @JsonProperty("error") Optional<String> error)
    {
        super(ok, error);
        this.latest = latest;
        this.messages = messages;
        this.hasMore = hasMore;
    }

    @JsonProperty
    public Optional<String> getLatest()
    {
        return latest;
    }

    @JsonProperty
    public Optional<List<SlackMessage>> getMessages()
    {
        return messages;
    }

    @JsonProperty("has_more")
    public Optional<Boolean> getHasMore()
    {
        return hasMore;
    }
}
