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
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHiveClientRetryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveClientRetryConfig.class)
                .setMaxMetastoreRetryAttempts(10)
                .setMetastoreRetryScaleFactor(2.0)
                .setMaxMetastoreRetryTime(new Duration(30, TimeUnit.SECONDS))
                .setMinMetastoreRetrySleepTime(new Duration(1, TimeUnit.SECONDS))
                .setMaxMetastoreRetrySleepTime(new Duration(1, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.client.create.max-retry-attempts", "20")
                .put("hive.metastore.client.create.retry-scale-fator", "1.5")
                .put("hive.metastore.client.create.max-retry-time", "2m")
                .put("hive.metastore.client.create.min-retry-sleep-time", "100ms")
                .put("hive.metastore.client.create.max-retry-sleep-time", "1500ms")
                .build();

        HiveClientRetryConfig expected = new HiveClientRetryConfig()
                .setMaxMetastoreRetryAttempts(20)
                .setMetastoreRetryScaleFactor(1.5)
                .setMaxMetastoreRetryTime(new Duration(2, TimeUnit.MINUTES))
                .setMinMetastoreRetrySleepTime(new Duration(100, TimeUnit.MILLISECONDS))
                .setMaxMetastoreRetrySleepTime(new Duration(1500, TimeUnit.MILLISECONDS));

        assertFullMapping(properties, expected);
    }
}
