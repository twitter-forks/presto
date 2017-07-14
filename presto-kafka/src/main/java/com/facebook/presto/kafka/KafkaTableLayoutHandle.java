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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import kafka.api.OffsetRequest;

import static java.util.Objects.requireNonNull;

public class KafkaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KafkaTableHandle table;
    private final Long offsetStartTs;
    private final Long offsetEndTs;

    @JsonCreator
    public KafkaTableLayoutHandle(
            @JsonProperty("table") KafkaTableHandle table,
            @JsonProperty("offset_start_ts") Long offsetStartTs,
            @JsonProperty("offset_end_ts") Long offsetEndTs)
    {
        this.table = requireNonNull(table, "table is null");
        this.offsetStartTs = offsetStartTs == null ? OffsetRequest.EarliestTime() : offsetStartTs;
        this.offsetEndTs = offsetEndTs == null ? OffsetRequest.LatestTime() : offsetEndTs;
    }

    @JsonProperty
    public KafkaTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Long getOffsetStartTs()
    {
        return offsetStartTs;
    }

    @JsonProperty
    public Long getOffsetEndTs()
    {
        return offsetEndTs;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
