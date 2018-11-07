package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.twitter.presto.maintenance.MaintenanceCoordinatorResource.DrainResponse;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.memory.TestMemoryManager.createQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.INTERNAL_SERVER_ERROR;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
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
                }
                else if (response.getStatusCode() == INTERNAL_SERVER_ERROR.code()) {
                    // 500 code indicates that the node is down and unreachable
                    break;
                }
                sleep(1000);
            }

            // HACK: we can't simulate lifecycle of individu
            Futures.allAsList(queryFutures).get();

            queryRunner.getCoordinator().getQueryManager().getAllQueryInfo().stream()
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
}
