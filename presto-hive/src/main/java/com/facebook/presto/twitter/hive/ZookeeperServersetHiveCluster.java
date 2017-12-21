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

import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClient;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ZookeeperServersetHiveCluster
        implements HiveCluster
{
    private static final Logger log = Logger.get(ZookeeperServersetHiveCluster.class);
    private final PooledHiveMetastoreClientFactory clientFactory;
    private ZookeeperMetastoreMonitor zkMetastoreMonitor;

    @Inject
    public ZookeeperServersetHiveCluster(ZookeeperServersetMetastoreConfig config, PooledHiveMetastoreClientFactory clientFactory)
            throws Exception
    {
        String zkServerHostAndPort = requireNonNull(config.getZookeeperServerHostAndPort(), "zkServerHostAndPort is null");
        String zkMetastorePath = requireNonNull(config.getZookeeperMetastorePath(), "zkMetastorePath is null");
        int zkRetries = requireNonNull(config.getZookeeperMaxRetries(), "zkMaxRetried is null");
        int zkRetrySleepTime = requireNonNull(config.getZookeeperRetrySleepTime(), "zkRetrySleepTime is null");
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.zkMetastoreMonitor = new ZookeeperMetastoreMonitor(zkServerHostAndPort, zkMetastorePath, zkRetries, zkRetrySleepTime);
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        List<HostAndPort> metastores = zkMetastoreMonitor.getServers();
        Collections.shuffle(metastores);
        TTransportException lastException = null;
        for (HostAndPort metastore : metastores) {
            try {
                log.info("Connecting to metastore at: %s", metastore.toString());
                return clientFactory.create(metastore.getHostText(), metastore.getPort());
            }
            catch (TTransportException e) {
                log.debug("Failed connecting to Hive metastore at: %s", metastore.toString());
                lastException = e;
            }
        }

        throw new RuntimeException("Failed connecting to Hive metastore.", lastException);
    }
}
