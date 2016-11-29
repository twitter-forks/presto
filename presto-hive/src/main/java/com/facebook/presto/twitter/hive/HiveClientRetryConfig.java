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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * This is a temporary fix for "Failed connecting to hive metastore" issue IQ-221.
 * Clean up if no need. Follow IQ-241
 *
 * @see IQ-221, IQ-241
 */
public class HiveClientRetryConfig
{
    private int maxMetastoreRetryAttempts = 10;
    private double metastoreRetryScaleFactor = 2.0;
    private Duration maxMetastoreRetryTime = new Duration(30, TimeUnit.SECONDS);
    private Duration minMetastoreRetrySleepTime = new Duration(1, TimeUnit.SECONDS);
    private Duration maxMetastoreRetrySleepTime = new Duration(1, TimeUnit.SECONDS);

    @Min(0)
    public int getMaxMetastoreRetryAttempts()
    {
        return maxMetastoreRetryAttempts;
    }

    @Config("hive.metastore.client.create.max-retry-attempts")
    public HiveClientRetryConfig setMaxMetastoreRetryAttempts(int maxMetastoreRetryAttempts)
    {
        this.maxMetastoreRetryAttempts = maxMetastoreRetryAttempts;
        return this;
    }

    @Min(1)
    public double getMetastoreRetryScaleFactor()
    {
        return metastoreRetryScaleFactor;
    }

    @Config("hive.metastore.client.create.retry-scale-fator")
    public HiveClientRetryConfig setMetastoreRetryScaleFactor(double metastoreRetryScaleFactor)
    {
        this.metastoreRetryScaleFactor = metastoreRetryScaleFactor;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMaxMetastoreRetryTime()
    {
        return maxMetastoreRetryTime;
    }

    @Config("hive.metastore.client.create.max-retry-time")
    public HiveClientRetryConfig setMaxMetastoreRetryTime(Duration maxMetastoreRetryTime)
    {
        this.maxMetastoreRetryTime = maxMetastoreRetryTime;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMinMetastoreRetrySleepTime()
    {
        return minMetastoreRetrySleepTime;
    }

    @Config("hive.metastore.client.create.min-retry-sleep-time")
    public HiveClientRetryConfig setMinMetastoreRetrySleepTime(Duration minMetastoreRetrySleepTime)
    {
        this.minMetastoreRetrySleepTime = minMetastoreRetrySleepTime;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMaxMetastoreRetrySleepTime()
    {
        return maxMetastoreRetrySleepTime;
    }

    @Config("hive.metastore.client.create.max-retry-sleep-time")
    public HiveClientRetryConfig setMaxMetastoreRetrySleepTime(Duration maxMetastoreRetrySleepTime)
    {
        this.maxMetastoreRetrySleepTime = maxMetastoreRetrySleepTime;
        return this;
    }
}
