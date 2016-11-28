/**
 * CachingHiveMetastoreRetryConfigurable is a subclass of CachingHiveMetastore.
 * It added retry configuration and overrided the RetryDriver setup method
 * retry().
 * The class added the flexibility to set up retry configuration from a
 * runtime injector. All retry configurations are propergated from a 
 * HiveClientRetryConfig instance.
 *
 * @author Yaliang Wang
 * 
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

import com.facebook.presto.hive.ForHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCluster;
import com.facebook.presto.hive.RetryDriver;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.spi.PrestoException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class CachingHiveMetastoreRetryConfigurable
        extends CachingHiveMetastore
{
    private static final Logger log = Logger.get(CachingHiveMetastoreRetryConfigurable.class);
    private int maxAttempts = 10;
    private double scaleFactor = 2.0;
    private Duration maxRetryTime = new Duration(30, SECONDS);
    private Duration minSleepTime = new Duration(1, SECONDS);
    private Duration maxSleepTime = new Duration(1, SECONDS);

    @Inject
    public CachingHiveMetastoreRetryConfigurable(
        HiveCluster hiveCluster,
        @ForHiveMetastore ExecutorService executor,
        HiveClientConfig hiveClientConfig,
        HiveClientRetryConfig hiveClientRetryConfig)
    {
        super(requireNonNull(hiveCluster, "hiveCluster is null"),
                requireNonNull(executor, "executor is null"),
                requireNonNull(hiveClientConfig, "hiveClientConfig is null").getMetastoreCacheTtl(),
                hiveClientConfig.getMetastoreRefreshInterval());
        metastoreClientRetryConfig(
            requireNonNull(hiveClientRetryConfig, "hiveClientRetryConfig is null").getMaxMetastoreRetryAttempts(),
            hiveClientRetryConfig.getMinMetastoreRetrySleepTime(),
            hiveClientRetryConfig.getMaxMetastoreRetrySleepTime(),
            hiveClientRetryConfig.getMaxMetastoreRetryTime(),
            hiveClientRetryConfig.getMetastoreRetryScaleFactor());
    }

    /**
     * Override RetryDriver setup method
     * @return A retry driver with retry configurations propergated from HiveClientRetryConfig.
     *           If the hiveClientRetryConfig is not set up, the default retry configurations 
     *           are used. And they are same as the default settings in RetryDriver class.
     */
    @Override
    protected RetryDriver retry()
    {
        log.debug(
            "Init a retry driver with attempts: %d, sleep time setting: (%s, %s, %s), scale factor: %f",
            maxAttempts,
            minSleepTime, maxSleepTime, maxRetryTime, scaleFactor);
        return RetryDriver.retry()
                .maxAttempts(maxAttempts)
                .exponentialBackoff(minSleepTime, maxSleepTime, maxRetryTime, scaleFactor)
                .exceptionMapper(getExceptionMapper())
                .stopOn(PrestoException.class);
    }

    /**
     * metastoreClientRetryConfig sets up all configurations for RetryDriver at one time.
     * @param maxAttempts  The max attempts for retry
     * @param minSleepTime The min interval between two attempts
     * @param maxSleepTime The max interval between two attempts
     * @param maxRetryTime The max time for retry
     * @param scaleFactor  The scale factor for sleep interval between two attempts
     */
    private void metastoreClientRetryConfig(
        int maxAttempts, Duration minSleepTime, Duration maxSleepTime, Duration maxRetryTime, double scaleFactor)
    {
        this.maxAttempts = maxAttempts;
        this.minSleepTime = requireNonNull(minSleepTime, "minSleepTime is null");
        this.maxSleepTime = requireNonNull(maxSleepTime, "maxSleepTime is null");
        this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
        this.scaleFactor = scaleFactor;
    }
}
