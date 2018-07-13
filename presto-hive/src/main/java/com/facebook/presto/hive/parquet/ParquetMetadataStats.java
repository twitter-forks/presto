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
package com.facebook.presto.hive.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ParquetMetadataStats
{
    private final DistributionStat metadataLength;
    private final DistributionStat readSize;
    private final Map<String, DistributionStat> dataReadSize;
    private final Map<String, DistributionStat> dictionaryReadSize;
    private final Map<String, DistributionStat> pageSize;

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public ParquetMetadataStats()
    {
        this.metadataLength = new DistributionStat();
        this.readSize = new DistributionStat();
        this.dataReadSize = new HashMap<>();
        this.dictionaryReadSize = new HashMap<>();
        this.pageSize = new HashMap<>();
    }

    @Managed
    @Nested
    public DistributionStat getMetadataLength()
    {
        return metadataLength;
    }

    @Managed
    @Nested
    public DistributionStat getReadSize()
    {
        return readSize;
    }

    @Managed
    public String getDataReadSize()
    {
        return getSnapshot(dataReadSize);
    }

    @Managed
    public String getDictionaryReadSize()
    {
        return getSnapshot(dictionaryReadSize);
    }

    @Managed
    public String getPageSize()
    {
        return getSnapshot(pageSize);
    }

    private static String getSnapshot(Map<String, DistributionStat> stats)
    {
        Map<String, DistributionStat.DistributionStatSnapshot> result = new LinkedHashMap<>(stats.size());
        for (Map.Entry<String, DistributionStat> entry : stats.entrySet()) {
            result.put(entry.getKey(), entry.getValue().snapshot());
        }
        return toJson(result);
    }

    private static String toJson(Map<String, DistributionStat.DistributionStatSnapshot> snapshot)
    {
        try {
            return objectMapper.writeValueAsString(snapshot);
        }
        catch (Exception ignore) {
            return snapshot.toString();
        }
    }

    public void addDataReadSize(String name, long size)
    {
        addSize(dataReadSize, name, size);
        readSize.add(size);
    }

    public void addDictionaryReadSize(String name, long size)
    {
        addSize(dictionaryReadSize, name, size);
        readSize.add(size);
    }

    public void addPageSize(String name, long size)
    {
        addSize(pageSize, name, size);
    }

    private static void addSize(Map<String, DistributionStat> stats, String name, long size)
    {
        if (!stats.containsKey(name)) {
            stats.put(name, new DistributionStat());
        }

        stats.get(name).add(size);
    }
}
