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
package com.twitter.presto.plugin.eventlistener.slack;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.twitter.presto.plugin.eventlistener.TestingTwitterEventListenerPlugin;
import com.twitter.presto.plugin.eventlistener.TwitterEventHandler;
import com.twitter.presto.plugin.eventlistener.TwitterEventListenerConfig;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;

@Test(singleThreaded = true)
public class TestSlackBot
{
    private static Logger log = Logger.get(TestSlackBot.class);
    private static final Optional<String> CREATED = Optional.of("Hi there, I just started a new query.");
    private static final Optional<String> FINISHED = Optional.of("I just completed your query.");
    private static final Optional<String> FAILED = Optional.of("Unfortunately, your query was failed due to error: ${FAILURE_MESSAGE}");

    private Map<String, SlackImOpenResponse> imOpenResponses = new HashMap<>();
    private Map<Map.Entry<String, String>, SlackChatPostMessageResponse> chatPostMessageResponses = new HashMap<>();
    private Map<String, SlackUsersLookupByEmailResponse> usersLookupByEmailResponses = new HashMap<>();
    private Map<Map.Entry<String, String>, SlackImHistoryResponse> imHistoryResponses = new HashMap<>();

    private DistributedQueryRunner queryRunner;
    private Session session;

    private TestingSlackResource resource;
    private LifeCycleManager lifeCycleManager;
    private TestingHttpServer server;

    @BeforeClass
    private void setUp()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                binder -> jaxrsBinder(binder).bind(TestingSlackResource.class));

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        server = injector.getInstance(TestingHttpServer.class);
        resource = injector.getInstance(TestingSlackResource.class);
        resource.initialize(imOpenResponses, chatPostMessageResponses, usersLookupByEmailResponses, imHistoryResponses);

        session = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1")
                .setCatalog("tpch")
                .setSchema("tiny")
                .setClientInfo("{\"clientVersion\":\"testVersion\"}")
                .build();

        TwitterEventListenerConfig config = new TwitterEventListenerConfig()
                .setSlackConfigFile(getResourceFilePath("slackCredentials.json"))
                .setSlackUri(uriFor(""))
                .setSlackEmailTemplate("${USER}@example.com")
                .setSlackNotificationTemplateFile(getResourceFilePath("slackNotifications.json"))
                .setSlackUsers("user");
        TwitterEventHandler handler = new SlackBot(config);

        queryRunner = new DistributedQueryRunner(session, 1);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new TestingTwitterEventListenerPlugin(handler));
        queryRunner.createCatalog("tpch", "tpch");
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
            throws Exception
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            // ignore
        }
        finally {
            lifeCycleManager = null;
        }

        try {
            queryRunner.close();
        }
        catch (Exception e) {
            // ignore
        }
        finally {
            queryRunner = null;
        }
    }

    @Test
    public void testConstantQuery()
            throws Exception
    {
        clearSlackResource();
        prepareSlackResource("user@example.com", "channel_id", "user_id", "user_name", CREATED, FINISHED, FAILED, Optional.empty());

        runQueryAndWaitForEvents(session, "SELECT 1", 8);

        assertEqualsIgnoreOrder(resource.getUsersLookupByEmailRequests(), ImmutableList.of("user@example.com", "user@example.com"));
        assertEqualsIgnoreOrder(resource.getImOpenRequests(), ImmutableList.of("user_id", "user_id"));
        assertEqualsIgnoreOrder(resource.getImHistoryRequests(), ImmutableList.of(getEntry("channel_id", null), getEntry("channel_id", null)));
        assertEqualsIgnoreOrder(resource.getChatPostMessageRequests(), ImmutableList.of(getEntry("channel_id", CREATED.get()), getEntry("channel_id", FINISHED.get())));
    }

    @Test
    public void testPartialStoppedChannel()
            throws Exception
    {
        clearSlackResource();
        List<SlackMessage> channelHistory = new ArrayList<>();
        channelHistory.add(new SlackMessage("stop event=created", "0"));
        prepareSlackResource("user@example.com", "channel_id", "user_id", "user_name", CREATED, FINISHED, FAILED, Optional.of(channelHistory));

        runQueryAndWaitForEvents(session, "SELECT 1", 7);

        assertEqualsIgnoreOrder(resource.getUsersLookupByEmailRequests(), ImmutableList.of("user@example.com", "user@example.com"));
        assertEqualsIgnoreOrder(resource.getImOpenRequests(), ImmutableList.of("user_id", "user_id"));
        assertEqualsIgnoreOrder(resource.getImHistoryRequests(), ImmutableList.of(getEntry("channel_id", null), getEntry("channel_id", null)));
        assertEqualsIgnoreOrder(resource.getChatPostMessageRequests(), ImmutableList.of(getEntry("channel_id", FINISHED.get())));
    }

    @Test
    public void testResumedChannel()
            throws Exception
    {
        clearSlackResource();

        List<SlackMessage> channelHistory = new ArrayList<>();
        channelHistory.add(new SlackMessage("resume event=created", "1"));
        channelHistory.add(new SlackMessage("stop event=created", "0"));

        prepareSlackResource("user@example.com", "channel_id", "user_id", "user_name", CREATED, FINISHED, FAILED, Optional.of(channelHistory));

        runQueryAndWaitForEvents(session, "SELECT 1", 8);

        assertEqualsIgnoreOrder(resource.getUsersLookupByEmailRequests(), ImmutableList.of("user@example.com", "user@example.com"));
        assertEqualsIgnoreOrder(resource.getImOpenRequests(), ImmutableList.of("user_id", "user_id"));
        assertEqualsIgnoreOrder(resource.getImHistoryRequests(), ImmutableList.of(getEntry("channel_id", null), getEntry("channel_id", null)));
        assertEqualsIgnoreOrder(resource.getChatPostMessageRequests(), ImmutableList.of(getEntry("channel_id", CREATED.get()), getEntry("channel_id", FINISHED.get())));
    }

    @Test
    public void testFailedQuery()
            throws Exception
    {
        clearSlackResource();
        prepareSlackResource("user@example.com", "channel_id", "user_id", "user_name", CREATED, Optional.empty(), Optional.empty(), Optional.empty());
        allowWildCardChatPostMessage();

        String failureMessage;
        // Run a query with Syntax Error
        try {
            runQueryAndWaitForEvents(session, "SELECT notexistcolumn FROM lineitem", 8);
            failureMessage = "";
        }
        catch (Exception e) {
            failureMessage = e.getMessage();
        }
        finally {
            // make sure it consumed all slack requests
            resource.waitForCalls(20);
        }

        assertEqualsIgnoreOrder(resource.getUsersLookupByEmailRequests(), ImmutableList.of("user@example.com", "user@example.com"));
        assertEqualsIgnoreOrder(resource.getImOpenRequests(), ImmutableList.of("user_id", "user_id"));
        assertEqualsIgnoreOrder(resource.getImHistoryRequests(), ImmutableList.of(getEntry("channel_id", null), getEntry("channel_id", null)));
        assertEqualsIgnoreOrder(resource.getChatPostMessageRequests(), ImmutableList.of(getEntry("channel_id", CREATED.get()), getEntry("channel_id", FAILED.get().replaceAll("\\$\\{FAILURE_MESSAGE}", failureMessage))));
    }

    private MaterializedResult runQueryAndWaitForEvents(Session session, String sql, int numCallsExpected)
            throws Exception
    {
        resource.setNumCallsExpected(numCallsExpected);
        MaterializedResult result = queryRunner.execute(session, sql);
        resource.waitForCalls(20);

        return result;
    }

    private void clearSlackResource()
    {
        usersLookupByEmailResponses.clear();
        imOpenResponses.clear();
        imHistoryResponses.clear();
        chatPostMessageResponses.clear();
    }

    private void prepareSlackResource(
            String email,
            String channel,
            String userId,
            String realName,
            Optional<String> createdMessage,
            Optional<String> finishedMessage,
            Optional<String> failedMessage,
            Optional<List<SlackMessage>> historyMessages)
    {
        usersLookupByEmailResponses.put(email, new SlackUsersLookupByEmailResponse(true, Optional.of(new SlackUser(userId, realName)), Optional.empty()));
        imOpenResponses.put(userId, new SlackImOpenResponse(true, Optional.of(new SlackChannel(channel)), Optional.empty()));

        SlackChatPostMessageResponse simpleOk = new SlackChatPostMessageResponse(true, Optional.empty(), Optional.empty());
        createdMessage.ifPresent(message -> chatPostMessageResponses.put(getEntry(channel, message), simpleOk));
        finishedMessage.ifPresent(message -> chatPostMessageResponses.put(getEntry(channel, message), simpleOk));
        failedMessage.ifPresent(message -> chatPostMessageResponses.put(getEntry(channel, message), simpleOk));

        imHistoryResponses.put(getEntry(channel, null), new SlackImHistoryResponse(true, Optional.empty(), historyMessages, Optional.of(false), Optional.empty()));
    }

    private void allowWildCardChatPostMessage()
    {
        chatPostMessageResponses.put(getEntry("*", "*"), new SlackChatPostMessageResponse(true, Optional.empty(), Optional.empty()));
    }

    private Map.Entry<String, String> getEntry(String key, String value)
    {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
