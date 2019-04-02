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
package com.twitter.presto.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestZookeeperMetastoreMonitor
{
    private static final Logger log = Logger.get(TestZookeeperMetastoreMonitor.class);

    private ZookeeperMetastoreMonitor zkMetastoreMonitor;
    private TestingServer zkServer;
    private ZkClient zkClient;
    private final String zkPath = "/metastores";

    public TestZookeeperMetastoreMonitor()
            throws Exception
    {
        zkServer = new TestingServer(findUnusedPort());
        zkClient = new ZkClient(zkServer.getConnectString(), 30_000, 30_000);

        // Set the serializer
        zkClient.setZkSerializer(new ZkSerializer()
        {
            @Override
            public byte[] serialize(Object o)
                    throws ZkMarshallingError
            {
                try {
                    return o.toString().getBytes(StandardCharsets.UTF_8);
                }
                catch (Exception e) {
                    log.warn("Exception in serializing " + e);
                }
                return "".getBytes();
            }

            @Override
            public Object deserialize(byte[] bytes)
                    throws ZkMarshallingError
            {
                return null;
            }
        });
    }

    @AfterClass
    public void destroy()
            throws IOException
    {
        zkMetastoreMonitor.close();
        zkClient.close();
        zkServer.close();
    }

    @BeforeTest
    public void setUp()
            throws Exception
    {
        log.info("Cleaning up zookeeper");
        zkClient.getChildren("/").stream()
                .filter(child -> !child.equals("zookeeper"))
                .forEach(child -> zkClient.deleteRecursive("/" + child));

        zkClient.unsubscribeAll();

        zkClient.createPersistent(zkPath);
        zkMetastoreMonitor = new ZookeeperMetastoreMonitor(zkServer.getConnectString(), zkPath, 3, 500);
    }

    @Test
    public void testGetServers()
            throws Exception
    {
        List<HostAndPort> servers;
        List<HostAndPort> expected;
        assertTrue(zkMetastoreMonitor.getServers().isEmpty());

        addServerToZk("nameNode1", "host1", 10001);
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = zkMetastoreMonitor.getServers();
        expected = ImmutableList.of(HostAndPort.fromParts("host1", 10001));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers));

        addServerToZk("nameNode2", "host2", 10002);
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = zkMetastoreMonitor.getServers();
        expected = ImmutableList.of(HostAndPort.fromParts("host1", 10001), HostAndPort.fromParts("host2", 10002));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers));

        // Change value of an existing name node
        addServerToZk("nameNode2", "host2", 10003);
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = zkMetastoreMonitor.getServers();
        expected = ImmutableList.of(HostAndPort.fromParts("host1", 10001), HostAndPort.fromParts("host2", 10003));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers));

        // Delete an existing name node
        zkClient.delete(getPathForNameNode("nameNode1"));
        // Sleep for some time so that event can be propagated.
        TimeUnit.MILLISECONDS.sleep(100);
        servers = zkMetastoreMonitor.getServers();
        expected = ImmutableList.of(HostAndPort.fromParts("host2", 10003));
        assertTrue(servers.containsAll(expected) && expected.containsAll(servers), servers.toString());
    }

    private void addServerToZk(String nameNode, String host, int port)
    {
        String path = getPathForNameNode(nameNode);

        if (!zkClient.exists(path)) {
            zkClient.createPersistent(path, toZookeeperEntry(host, port));
        }
        else {
            zkClient.writeData(path, toZookeeperEntry(host, port));
        }
    }

    private String getPathForNameNode(String nameNode)
    {
        return zkPath + "/" + nameNode;
    }

    private static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String toZookeeperEntry(String host, int port)
    {
        return format("{\"serviceEndpoint\" : {\"host\" : \"%s\", \"port\" : %d}}", host, port);
    }
}
