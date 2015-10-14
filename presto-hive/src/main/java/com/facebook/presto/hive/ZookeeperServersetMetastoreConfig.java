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
package com.facebook.presto.hive;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class ZookeeperServersetMetastoreConfig
{
    private String zookeeperServerHostAndPort;
    private String zookeeperMetastorePath;
    private int zookeeperRetrySleepTime = 500; // ms
    private int zookeeperMaxRetries = 3;

    public String getZookeeperServerHostAndPort()
    {
        return zookeeperServerHostAndPort;
    }

    @Config("hive.metastore.zookeeper.uri")
    @ConfigDescription("Zookeeper Host and Port")
    public ZookeeperServersetMetastoreConfig setZookeeperServerHostAndPort(String zookeeperServerHostAndPort)
    {
        this.zookeeperServerHostAndPort = zookeeperServerHostAndPort;
        return this;
    }

    public String getZookeeperMetastorePath()
    {
        return zookeeperMetastorePath;
    }

    @Config("hive.metastore.zookeeper.path")
    @ConfigDescription("Hive metastore Zookeeper path")
    public ZookeeperServersetMetastoreConfig setZookeeperMetastorePath(String zkPath)
    {
        this.zookeeperMetastorePath = zkPath;
        return this;
    }

    @NotNull
    public int getZookeeperRetrySleepTime()
    {
        return zookeeperRetrySleepTime;
    }

    @Config("hive.metastore.zookeeper.retry.sleeptime")
    @ConfigDescription("Zookeeper sleep time between reties")
    public ZookeeperServersetMetastoreConfig setZookeeperRetrySleepTime(int zookeeperRetrySleepTime)
    {
        this.zookeeperRetrySleepTime = zookeeperRetrySleepTime;
        return this;
    }

    @Min(1)
    public int getZookeeperMaxRetries()
    {
        return zookeeperMaxRetries;
    }

    @Config("hive.metastore.zookeeper.max.retries")
    @ConfigDescription("Zookeeper max reties")
    public ZookeeperServersetMetastoreConfig setZookeeperMaxRetries(int zookeeperMaxRetries)
    {
        this.zookeeperMaxRetries = zookeeperMaxRetries;
        return this;
    }
}
