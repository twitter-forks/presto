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

import com.facebook.presto.spi.PrestoException;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Created by smittal on 10/9/15.
 */
public class ZookeeperServersetHiveCluster
        implements HiveCluster
{
    private static final Logger log = Logger.get(ZookeeperServersetHiveCluster.class);
    private final HiveMetastoreClientFactory clientFactory;
    private ZookeeperMetastoreMonitor zkMetastoreMonitor;

    @Inject
    public ZookeeperServersetHiveCluster(ZookeeperServersetMetastoreConfig config, HiveMetastoreClientFactory clientFactory)
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
        TTransportException lastException = null;
        for (HostAndPort metastore : metastores) {
            try {
                log.info("Connecting to metastore at: " + metastore.toString());
                return clientFactory.create(metastore.getHostText(), metastore.getPort());
            }
            catch (TTransportException e) {
                lastException = e;
            }
        }

        throw new PrestoException(HIVE_METASTORE_ERROR, "Failed connecting to Hive metastore", lastException);
    }
}
