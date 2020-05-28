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
package com.twitter.presto.plugin.eventlistener;

import com.facebook.presto.Session;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTwitterEventListener
{
    private static final int SPLITS_PER_NODE = 3;
    private final TestingTwitterEventHandler handler = new TestingTwitterEventHandler();

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
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", Integer.toString(SPLITS_PER_NODE)));
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
        // QueryCreated: 1, QueryCompleted: 1, Splits: 1
        runQueryAndWaitForEvents("SELECT 1", 3);

        QueryCreatedEvent queryCreatedEvent = getOnlyElement(handler.getQueryCreatedEvents());
        assertEquals(queryCreatedEvent.getContext().getServerVersion(), "testversion");
        assertEquals(queryCreatedEvent.getContext().getServerAddress(), "127.0.0.1");
        assertEquals(queryCreatedEvent.getContext().getEnvironment(), "testing");
        assertEquals(queryCreatedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQuery(), "SELECT 1");

        QueryCompletedEvent queryCompletedEvent = getOnlyElement(handler.getQueryCompletedEvents());
        assertEquals(queryCompletedEvent.getStatistics().getTotalRows(), 0L);
        assertEquals(queryCompletedEvent.getContext().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");
        assertEquals(queryCreatedEvent.getMetadata().getQueryId(), queryCompletedEvent.getMetadata().getQueryId());

        List<SplitCompletedEvent> splitCompletedEvents = handler.getSplitCompletedEvents();
        assertEquals(splitCompletedEvents.get(0).getQueryId(), queryCompletedEvent.getMetadata().getQueryId());
        assertEquals(splitCompletedEvents.get(0).getStatistics().getCompletedPositions(), 1);
    }

    private MaterializedResult runQueryAndWaitForEvents(String sql, int numEventsExpected)
            throws Exception
    {
        handler.initialize(numEventsExpected);
        MaterializedResult result = queryRunner.execute(session, sql);
        handler.waitForEvents(10);

        return result;
    }

    static class TestingTwitterEventHandler
            implements TwitterEventHandler
    {
        private ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents;
        private ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents;
        private ImmutableList.Builder<SplitCompletedEvent> splitCompletedEvents;

        private CountDownLatch eventsLatch;

        public synchronized void initialize(int numEvents)
        {
            queryCreatedEvents = ImmutableList.builder();
            queryCompletedEvents = ImmutableList.builder();
            splitCompletedEvents = ImmutableList.builder();

            eventsLatch = new CountDownLatch(numEvents);
        }

        public void waitForEvents(int timeoutSeconds)
                throws InterruptedException
        {
            eventsLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        public synchronized void addQueryCreated(QueryCreatedEvent event)
        {
            queryCreatedEvents.add(event);
            eventsLatch.countDown();
        }

        public synchronized void addQueryCompleted(QueryCompletedEvent event)
        {
            queryCompletedEvents.add(event);
            eventsLatch.countDown();
        }

        public synchronized void addSplitCompleted(SplitCompletedEvent event)
        {
            splitCompletedEvents.add(event);
            eventsLatch.countDown();
        }

        public List<QueryCreatedEvent> getQueryCreatedEvents()
        {
            return queryCreatedEvents.build();
        }

        public List<QueryCompletedEvent> getQueryCompletedEvents()
        {
            return queryCompletedEvents.build();
        }

        public List<SplitCompletedEvent> getSplitCompletedEvents()
        {
            return splitCompletedEvents.build();
        }

        public void handleQueryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            addQueryCreated(queryCreatedEvent);
        }

        public void handleQueryCompleted(QueryCompletedEvent queryCompletedEvent)
        {
            addQueryCompleted(queryCompletedEvent);
        }

        public void handleSplitCompleted(SplitCompletedEvent splitCompletedEvent)
        {
            addSplitCompleted(splitCompletedEvent);
        }
    }
}
