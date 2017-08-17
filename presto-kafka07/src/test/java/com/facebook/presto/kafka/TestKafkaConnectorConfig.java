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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

public class TestKafkaConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(KafkaConnectorConfig.class)
                .setNodes("")
                .setKafkaConnectTimeout("10s")
                .setKafkaBufferSize("64kB")
                .setDefaultSchema("default")
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/kafka07/"))
                .setHideInternalColumns(true)
                .setFetchSize(10485760)
                .setZkEndpoint("")
                .setDefaultQueryInterval(10 * 60 * 1000)
                .setHardLimitOn(true)
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.table-description-dir", "/var/lib/kafka07")
                .put("kafka.table-names", "table1, table2, table3")
                .put("kafka.default-schema", "kafka")
                .put("kafka.nodes", "localhost:12345,localhost:23456")
                .put("kafka.connect-timeout", "1h")
                .put("kafka.buffer-size", "1MB")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.fetch-size", "10000000")
                .put("kafka.zk-endpoint", "localhost:2181")
                .put("kafka.default-query-interval", "3600000")
                .put("kafka.hardlimit-on", "false")
                .build();

        KafkaConnectorConfig expected = new KafkaConnectorConfig()
                .setTableDescriptionDir(new File("/var/lib/kafka07"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("kafka")
                .setNodes("localhost:12345, localhost:23456")
                .setKafkaConnectTimeout("1h")
                .setKafkaBufferSize("1MB")
                .setHideInternalColumns(false)
                .setFetchSize(10000000)
                .setZkEndpoint("localhost:2181")
                .setDefaultQueryInterval(3600000)
                .setHardLimitOn(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
