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
package com.twitter.presto.plugin.eventlistener.scriber;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.twitter.logpipeline.client.serializers.EventLogMsgSerializer;
import com.twitter.logpipeline.client.serializers.EventLogMsgTBinarySerializer;
import com.twitter.logpipeline.client.serializers.EventLogSerializationException;
import com.twitter.presto.plugin.eventlistener.TestingTwitterEventListenerPlugin;
import com.twitter.presto.plugin.eventlistener.TwitterEventHandler;
import com.twitter.presto.thriftjava.QueryCompletionEvent;
import com.twitter.presto.thriftjava.QueryStageInfo;
import com.twitter.presto.thriftjava.QueryState;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestQueryCompletedEventScriber
{
    private static final TDeserializer tDeserializer = new TDeserializer();
    // Currently, there is no way to pass principal from test client.
    private static final Identity identity = new Identity("test_user", Optional.empty());
    private final TestingTwitterScriber scriber = new TestingTwitterScriber();
    private final TwitterEventHandler handler = new QueryCompletedEventScriber(scriber);

    private DistributedQueryRunner queryRunner;
    private Session session;

    @BeforeClass
    private void setUp()
            throws Exception
    {
        session = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1")
                .setCatalog("tpch")
                .setSchema("tiny")
                .setClientInfo("{\"clientVersion\":\"testVersion\"}")
                .build();
        queryRunner = new DistributedQueryRunner(session, 1);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new TestingTwitterEventListenerPlugin(handler));
        queryRunner.createCatalog("tpch", "tpch");
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testConstantQuery()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setIdentity(identity)
                .build();
        runQueryAndWaitForEvents(session, "SELECT 1", 1);

        String queryCompletedEvent = getOnlyElement(scriber.getMessages());
        QueryCompletionEvent tEvent = new QueryCompletionEvent();
        tDeserializer.deserialize(tEvent, Base64.getDecoder().decode(queryCompletedEvent));

        // check user audit information
        assertEquals(tEvent.getUser(), identity.getUser());

        // check server audit information
        assertEquals(tEvent.getSource(), "test");
        assertEquals(tEvent.getServer_version(), "testversion");
        assertEquals(tEvent.getEnvironment(), "testing");

        // check query audit information
        assertEquals(tEvent.getCatalog(), "tpch");
        assertEquals(tEvent.getSchema(), "tiny");
        assertEquals(tEvent.getQuery(), "SELECT 1");
        assertEquals(tEvent.getQuery_state(), QueryState.FINISHED);

        // check query stats information
        assertEquals(tEvent.getTotal_rows(), 0L);
        assertEquals(tEvent.getTotal_bytes(), 0L);
    }

    @Test
    public void testNormalQuery()
            throws Exception
    {
        runQueryAndWaitForEvents(session, "SELECT sum(linenumber) FROM lineitem", 1);

        String queryCompletedEvent = getOnlyElement(scriber.getMessages());
        QueryCompletionEvent tEvent = new QueryCompletionEvent();
        tDeserializer.deserialize(tEvent, Base64.getDecoder().decode(queryCompletedEvent));

        // check query audit information
        assertEquals(tEvent.getQueried_columns_by_table(), ImmutableMap.of("sf0.01.lineitem", ImmutableList.of("linenumber")));
        assertEquals(tEvent.getQuery_state(), QueryState.FINISHED);

        // check query state information
        assertEquals(tEvent.getTotal_rows(), 60175L);
        assertEquals(tEvent.getTotal_bytes(), 0L);
        assertEquals(tEvent.getSplits(), tEvent.getQuery_stages().values().stream()
                .map(QueryStageInfo::getCompleted_drivers)
                .reduce(0, Integer::sum).intValue());
        assertEquals(tEvent.getCpu_time_ms(), tEvent.getQuery_stages().values().stream()
                .map(QueryStageInfo::getTotal_cpu_time_millis)
                .reduce(0L, Long::sum).longValue());
    }

    @Test
    public void testFailedQuery()
            throws Exception
    {
        String failureMessage;
        // Run a query with Syntax Error
        try {
            runQueryAndWaitForEvents(session, "SELECT notexistcolumn FROM lineitem", 1);
            failureMessage = "";
        }
        catch (Exception e) {
            failureMessage = e.getMessage();
        }

        String queryCompletedEvent = getOnlyElement(scriber.getMessages());
        QueryCompletionEvent tEvent = new QueryCompletionEvent();
        tDeserializer.deserialize(tEvent, Base64.getDecoder().decode(queryCompletedEvent));

        // check query failure information
        assertEquals(tEvent.getQuery_state(), QueryState.FAILED);
        assertEquals(tEvent.getError_code_id(), 1);
        assertEquals(tEvent.getError_code_name(), "SYNTAX_ERROR");
        assertEquals(tEvent.getFailure_message(), failureMessage);
    }

    private MaterializedResult runQueryAndWaitForEvents(Session session, String sql, int numMessagesExpected)
            throws Exception
    {
        scriber.initialize(numMessagesExpected);
        MaterializedResult result = queryRunner.execute(session, sql);
        scriber.waitForEvents(10);

        return result;
    }

    private class TestingTwitterScriber
            extends TwitterEventPublisher
    {
        private final List<String> messages;
        private CountDownLatch messagesLatch;

        public TestingTwitterScriber()
        {
            super("test");
            this.messages = new ArrayList<>();
        }

        public synchronized void initialize(int numEvents)
        {
            messagesLatch = new CountDownLatch(numEvents);
            messages.clear();
        }

        public void waitForEvents(int timeoutSeconds)
                throws InterruptedException
        {
            messagesLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        public List<String> getMessages()
        {
            return ImmutableList.copyOf(messages);
        }

        @Override
        public void scribe(QueryCompletionEvent thriftMessage)
                throws TException
        {
            EventLogMsgSerializer<TBase> serializer = EventLogMsgTBinarySerializer.getNewSerializer();
            try {
                messages.add(Base64.getEncoder().encodeToString(serializer.serialize(thriftMessage)));
            }
            catch (EventLogSerializationException e) {
                e.printStackTrace();
                throw new TException(e.getMessage());
            }
            messagesLatch.countDown();
        }
    }
}
