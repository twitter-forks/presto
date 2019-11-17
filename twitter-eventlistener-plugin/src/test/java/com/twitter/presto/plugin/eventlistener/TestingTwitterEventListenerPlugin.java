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

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingTwitterEventListenerPlugin
        implements Plugin
{
    private EventListenerFactory factory;

    public TestingTwitterEventListenerPlugin(TwitterEventHandler... handlers)
    {
        this.factory = new TestingTwitterEventListenerFactory(requireNonNull(handlers, "handler is null"));
    }

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return ImmutableList.of(factory);
    }

    private class TestingTwitterEventListenerFactory
            implements EventListenerFactory
    {
        private TwitterEventHandler[] handlers;

        public TestingTwitterEventListenerFactory(TwitterEventHandler... handlers)
        {
            for (TwitterEventHandler handler : handlers) {
                requireNonNull(handler, format("handler is null"));
            }
            this.handlers = handlers;
        }

        @Override
        public String getName()
        {
            return "testing-twitter-event-listener";
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return new TwitterEventListener(ImmutableSet.copyOf(handlers));
        }
    }
}
