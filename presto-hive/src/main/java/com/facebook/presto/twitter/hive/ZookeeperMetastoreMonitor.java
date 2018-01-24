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

import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ZookeeperMetastoreMonitor
        implements PathChildrenCacheListener
{
    public static final Logger log = Logger.get(ZookeeperMetastoreMonitor.class);
    private CuratorFramework client;
    private PathChildrenCache cache;
    private ConcurrentMap<String, HostAndPort> servers;  // (Node_Name->HostAndPort)

    public ZookeeperMetastoreMonitor(String zkServer, String watchPath, int maxRetries, int retrySleepTime)
            throws Exception
    {
        client = CuratorFrameworkFactory.newClient(zkServer, new ExponentialBackoffRetry(retrySleepTime, maxRetries));
        client.start();

        cache = new PathChildrenCache(client, watchPath, true); // true indicating cache node contents in addition to the stat
        try {
            cache.start();
        }
        catch (Exception ex) {
            throw new RuntimeException("Curator PathCache Creation failed: " + ex.getMessage());
        }

        cache.getListenable().addListener(this);
        servers = new ConcurrentHashMap<>();
    }

    public void close()
    {
        client.close();

        try {
            cache.close();
        }
        catch (IOException ex) {
            // do nothing
        }
    }

    public List<HostAndPort> getServers()
    {
        return servers.values().stream().collect(Collectors.toList());
    }

    private HostAndPort deserialize(byte[] bytes)
    {
        String serviceEndpoint = "serviceEndpoint";
        JSONObject data = (JSONObject) JSONValue.parse(new String(bytes));
        if (data != null && data.containsKey(serviceEndpoint)) {
            Map<String, Object> hostPortMap = (Map) data.get(serviceEndpoint);
            String host = hostPortMap.get("host").toString();
            int port = Integer.parseInt(hostPortMap.get("port").toString());
            return HostAndPort.fromParts(host, port);
        }
        else {
            log.warn("failed to deserialize child node data");
            throw new IllegalArgumentException("No host:port found");
        }
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        switch (event.getType()) {
            case CHILD_ADDED:
            case CHILD_UPDATED: {
                HostAndPort hostPort = deserialize(event.getData().getData());
                String node = ZKPaths.getNodeFromPath(event.getData().getPath());
                log.info("child updated: " + node + ": " + hostPort);
                servers.put(node, hostPort);
                break;
            }

            case CHILD_REMOVED: {
                String node = ZKPaths.getNodeFromPath(event.getData().getPath());
                log.info("child removed: " + node);
                servers.remove(node);
                break;
            }

            default:
                log.info("connection state changed: " + event.getType());
                break;
        }
    }
}
