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
package com.twitter.presto.tests;

import com.facebook.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.twitter.presto.maintenance.MaintenanceCoordinatorResource.DrainResponse;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static com.facebook.airlift.http.client.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.memory.TestMemoryManager.createQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMaintenanceModule
{
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 240_000;
    private static final JsonCodec<DrainResponse> DRAIN_RESPONSE_CODEC = JsonCodec.jsonCodec(DrainResponse.class);
    private static final JsonCodec<NodeState> NODE_STATE_CODEC = jsonCodec(NodeState.class);

    private static final Session TINY_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();
    private ListeningExecutorService executor;
    private HttpClient client;

    @BeforeClass
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
        client = new JettyHttpClient();
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testMaintenanceModule()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.include-coordinator", "false")
                .put("shutdown.grace-period", "1s")
                .put("maintenance.coordinator", "true")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
            }

            // pick a random worker for maintenance
            TestingPrestoServer worker = queryRunner.getServers()
                    .stream()
                    .filter(server -> !server.isCoordinator())
                    .findAny()
                    .get();

            TaskManager taskManager = worker.getTaskManager();
            // wait until tasks show up on the worker
            while (taskManager.getAllTaskInfo().isEmpty()) {
                MILLISECONDS.sleep(500);
            }

            // try drain the worker
            while (true) {
                JsonResponse<DrainResponse> response = tryDrain(queryRunner.getCoordinator().getBaseUrl(), worker.getBaseUrl());
                if (response.getStatusCode() == OK.code()) {
                    assertTrue(response.getValue().getDrain() == false);
                    // check the remote node state to make sure node is shutting down
                    assertTrue(getNodeState(worker.getBaseUrl()) == NodeState.SHUTTING_DOWN);
                }
                else if (response.getStatusCode() == INTERNAL_SERVER_ERROR.code()) {
                    // 500 code indicates that the node is down and unreachable
                    break;
                }
                sleep(1000);
            }

            // HACK: we can't simulate lifecycle of individu
            Futures.allAsList(queryFutures).get();

            queryRunner.getCoordinator().getQueryManager().getQueries().stream()
                    .forEach(x -> assertEquals(x.getState(), FINISHED));

            TestingPrestoServer.TestShutdownAction shutdownAction = (TestingPrestoServer.TestShutdownAction) worker.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertTrue(shutdownAction.isWorkerShutdown());
        }
    }

    private JsonResponse<DrainResponse> tryDrain(URI coordinatorUri, URI targetUri)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{ \"taskConfig\": { \"assignedTask\": { \"slaveHost\": \"")
                .append(targetUri.getHost())
                .append("\", \"assignedPorts\": { \"http\": ")
                .append(targetUri.getPort())
                .append(" } } } }");

        Request request = preparePost()
                .setUri(uriBuilderFrom(coordinatorUri).appendPath("/canDrain").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(createStaticBodyGenerator(stringBuilder.toString(), UTF_8))
                .build();
        return client.execute(request, createFullJsonResponseHandler(DRAIN_RESPONSE_CODEC));
    }

    private NodeState getNodeState(URI nodeUri)
    {
        URI nodeStateUri = uriBuilderFrom(nodeUri).appendPath("/v1/info/state").build();
        // synchronously send SHUTTING_DOWN request to worker node
        Request request = prepareGet()
                .setUri(nodeStateUri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        return client.execute(request, createJsonResponseHandler(NODE_STATE_CODEC));
    }
}
