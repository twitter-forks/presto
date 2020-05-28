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

import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.HiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreClient;
import com.facebook.presto.twitter.hive.util.PooledTTransportFactory;
import com.facebook.presto.twitter.hive.util.TTransportPool;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PooledHiveMetastoreClientFactory
{
    private final HostAndPort socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final TTransportPool transportPool;

    public PooledHiveMetastoreClientFactory(@Nullable HostAndPort socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            int maxTransport,
            long idleTimeout,
            long transportEvictInterval,
            int evictNumTests)
    {
        this.socksProxy = socksProxy;
        this.timeoutMillis = toIntExact(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(maxTransport);
        poolConfig.setMaxTotal(maxTransport);
        poolConfig.setMinEvictableIdleTimeMillis(idleTimeout);
        poolConfig.setTimeBetweenEvictionRunsMillis(transportEvictInterval);
        poolConfig.setNumTestsPerEvictionRun(evictNumTests);
        this.transportPool = new TTransportPool(poolConfig);
    }

    @Inject
    public PooledHiveMetastoreClientFactory(MetastoreClientConfig config,
            ZookeeperServersetMetastoreConfig zkConfig,
            HiveMetastoreAuthentication metastoreAuthentication)
    {
        this(config.getMetastoreSocksProxy(),
                config.getMetastoreTimeout(),
                metastoreAuthentication,
                zkConfig.getMaxTransport(),
                zkConfig.getTransportIdleTimeout(),
                zkConfig.getTransportEvictInterval(),
                zkConfig.getTransportEvictNumTests());
    }

    public HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        try {
            TTransport transport = transportPool.borrowObject(host, port);
            if (transport == null) {
                transport = transportPool.borrowObject(host, port,
                    new PooledTTransportFactory(transportPool,
                                                host, port, socksProxy,
                                                timeoutMillis, metastoreAuthentication));
            }
            return new ThriftHiveMetastoreClient(transport);
        }
        catch (Exception e) {
            throw new TTransportException(String.format("%s: %s", host, e.getMessage()), e.getCause());
        }
    }
}
