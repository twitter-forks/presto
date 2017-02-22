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
package com.facebook.presto.twitter.hive;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestZookeeperServersetMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ZookeeperServersetMetastoreConfig.class)
                .setZookeeperMaxRetries(3)
                .setZookeeperRetrySleepTime(500)
                .setZookeeperMetastorePath(null)
                .setZookeeperServerHostAndPort(null)
                .setMaxTransport(128)
                .setTransportIdleTimeout(300_000L)
                .setTransportEvictInterval(10_000L)
                .setTransportEvictNumTests(3)
                .setEnableConnectionPool(true));
    }

    @Test
    public void testExplicitPropertyMappingsSingleMetastore()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.zookeeper.uri", "localhost:2181")
                .put("hive.metastore.zookeeper.path", "/zookeeper/path/")
                .put("hive.metastore.zookeeper.retry.sleeptime", "200")
                .put("hive.metastore.zookeeper.max.retries", "2")
                .put("hive.metastore.max-transport-num", "64")
                .put("hive.metastore.transport-idle-timeout", "100000")
                .put("hive.metastore.transport-eviction-interval", "1000")
                .put("hive.metastore.transport-eviction-num-tests", "10")
                .put("hive.metastore.transport-pool-enable", "false")
                .build();

        ZookeeperServersetMetastoreConfig expected = new ZookeeperServersetMetastoreConfig()
                .setZookeeperServerHostAndPort("localhost:2181")
                .setZookeeperMetastorePath("/zookeeper/path/")
                .setZookeeperRetrySleepTime(200)
                .setZookeeperMaxRetries(2)
                .setMaxTransport(64)
                .setTransportIdleTimeout(100_000L)
                .setTransportEvictInterval(1_000L)
                .setTransportEvictNumTests(10)
                .setEnableConnectionPool(false);

        assertFullMapping(properties, expected);
    }
}
