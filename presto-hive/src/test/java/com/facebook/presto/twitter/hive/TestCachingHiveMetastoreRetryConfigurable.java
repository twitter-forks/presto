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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCluster;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.MockHiveMetastoreClient;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

/**
 * Test the CachingHiveMetastoreRetryConfigurable.
 *
 * This is a temporary fix for "Failed connecting to hive metastore" issue IQ-221.
 * Clean up if no need. Follow IQ-241
 *
 * @see IQ-221, IQ-241
 */
@Test(singleThreaded = true)
public class TestCachingHiveMetastoreRetryConfigurable
{
    private MockHiveMetastoreClient mockClient;
    private MockHiveCluster mockHiveCluster;
    private HiveMetastore metastore;
    static final int MAX_RETRY_ATTEMPTS = 7;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        mockClient = new MockHiveMetastoreClient();
        mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        HiveClientConfig hiveClientConfig = new HiveClientConfig()
                .setMetastoreCacheTtl(new Duration(5, TimeUnit.MINUTES))
                .setMetastoreRefreshInterval(new Duration(1, TimeUnit.MINUTES));
        HiveClientRetryConfig retryConfig = new HiveClientRetryConfig()
                .setMaxMetastoreRetryAttempts(MAX_RETRY_ATTEMPTS)
                .setMetastoreRetryScaleFactor(1.2)
                .setMaxMetastoreRetryTime(new Duration(1, TimeUnit.MINUTES))
                .setMinMetastoreRetrySleepTime(new Duration(100, TimeUnit.MILLISECONDS))
                .setMaxMetastoreRetrySleepTime(new Duration(500, TimeUnit.MILLISECONDS));
        metastore = new CachingHiveMetastoreRetryConfigurable(mockHiveCluster, executor, hiveClientConfig, retryConfig);
    }

    @Test
    public void testRetryAttempts()
        throws Exception
    {
        // Try if the failure time is greater than or equal max retry attemps
        mockHiveCluster.setFailToConnectMetastore(true, MAX_RETRY_ATTEMPTS + 1);
        mockHiveCluster.resetAccessCount();
        try {
            metastore.getAllDatabases();
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockHiveCluster.getAccessCount(), MAX_RETRY_ATTEMPTS);

        // Try if the failure time is less than max retry attempts
        mockHiveCluster.setFailToConnectMetastore(true, MAX_RETRY_ATTEMPTS - 1);
        mockHiveCluster.resetAccessCount();
        assertEquals(metastore.getAllDatabases(), ImmutableList.of("testdb"));
        assertEquals(mockHiveCluster.getAccessCount(), MAX_RETRY_ATTEMPTS);

        mockHiveCluster.setFailToConnectMetastore(false, 0);
    }

    private static class MockHiveCluster
            implements HiveCluster
    {
        private final HiveMetastoreClient client;
        private boolean failToConnectMetastore = false;
        private int failAttempts = 0;
        private final AtomicInteger accessCount = new AtomicInteger();

        private MockHiveCluster(HiveMetastoreClient client)
        {
            this.client = client;
        }

        public void setFailToConnectMetastore(boolean failToConnectMetastore, int failAttempts)
        {
            this.failToConnectMetastore = failToConnectMetastore;
            this.failAttempts = failAttempts;
        }

        public void resetAccessCount()
        {
            accessCount.set(0);
        }

        public int getAccessCount()
        {
            return accessCount.get();
        }

        @Override
        public HiveMetastoreClient createMetastoreClient()
        {
            accessCount.incrementAndGet();
            if (failToConnectMetastore && accessCount.get() <= failAttempts) {
                throw new RuntimeException("Failed connecting to Hive metastore.");
            }
            return client;
        }
    }
}
