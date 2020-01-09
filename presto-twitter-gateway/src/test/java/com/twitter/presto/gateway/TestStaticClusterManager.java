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
package com.twitter.presto.gateway;

import com.facebook.presto.execution.QueryState;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.twitter.presto.gateway.cluster.ClusterStatusTracker;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Logging;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStaticClusterManager
{
    private static final int NUM_CLUSTERS = 2;
    private static final int NUM_QUERIES = 7;

    private List<TestingPrestoServer> prestoServers;
    private LifeCycleManager lifeCycleManager;
    private HttpServerInfo httpServerInfo;
    private ClusterStatusTracker clusterStatusTracker;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        ImmutableList.Builder builder = ImmutableList.builder();
        for (int i = 0; i < NUM_CLUSTERS; ++i) {
            builder.add(createPrestoServer());
        }
        prestoServers = builder.build();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new GatewayModule());

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperty("gateway.version", "testversion")
                .setRequiredConfigurationProperty("gateway.cluster-manager.type", "STATIC")
                .setRequiredConfigurationProperty("gateway.cluster-manager.static.cluster-list", getClusterList(prestoServers))
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        httpServerInfo = injector.getInstance(HttpServerInfo.class);
        clusterStatusTracker = injector.getInstance(ClusterStatusTracker.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        for (TestingPrestoServer prestoServer : prestoServers) {
            prestoServer.close();
        }
        lifeCycleManager.stop();
    }

    @Test
    public void testQuery()
            throws Exception
    {
        for (int i = 0; i < NUM_QUERIES; ++i) {
            try (Connection connection = createConnection(httpServerInfo.getHttpUri());
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT row_number() OVER () n FROM tpch.tiny.orders")) {
                long count = 0;
                long sum = 0;
                while (rs.next()) {
                    count++;
                    sum += rs.getLong("n");
                }
                assertEquals(count, 15000);
                assertEquals(sum, (count / 2) * (1 + count));
            }
        }

        sleepUninterruptibly(10, SECONDS);
        assertEquals(clusterStatusTracker.getAllQueryInfos().size(), NUM_QUERIES);
        assertQueryState();
    }

    private void assertQueryState()
            throws SQLException
    {
        String sql = "SELECT query_id, state FROM system.runtime.queries";
        int total = 0;
        for (TestingPrestoServer server : prestoServers) {
            try (Connection connection = createConnection(server.getBaseUrl());
                     Statement statement = connection.createStatement();
                     ResultSet rs = statement.executeQuery(sql)) {
                String id = rs.unwrap(PrestoResultSet.class).getQueryId();
                int count = 0;
                while (rs.next()) {
                    if (!rs.getString("query_id").equals(id)) {
                        assertEquals(QueryState.valueOf(rs.getString("state")), QueryState.FINISHED);
                        count++;
                    }
                }
                assertTrue(count > 0);
                total += count;
            }
        }
        assertEquals(total, NUM_QUERIES);
    }

    private static TestingPrestoServer createPrestoServer()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.refreshNodes();

        return server;
    }

    private static String getClusterList(List<TestingPrestoServer> servers)
    {
        return servers.stream()
            .map(TestingPrestoServer::getBaseUrl)
            .map(URI::toString)
            .collect(Collectors.joining(","));
    }

    private static Connection createConnection(URI uri)
            throws SQLException
    {
        String url = format("jdbc:presto://%s:%s", uri.getHost(), uri.getPort());
        return DriverManager.getConnection(url, "test", null);
    }
}
