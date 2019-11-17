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

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TwitterEventListener
        implements EventListener
{
    private final Set<TwitterEventHandler> handlers;

    @Inject
    public TwitterEventListener(Set<TwitterEventHandler> handlers)
    {
        this.handlers = requireNonNull(handlers, "handlers is null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        for (TwitterEventHandler handler : handlers) {
            handler.handleQueryCreated(queryCreatedEvent);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        for (TwitterEventHandler handler : handlers) {
            handler.handleQueryCompleted(queryCompletedEvent);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        for (TwitterEventHandler handler : handlers) {
            handler.handleSplitCompleted(splitCompletedEvent);
        }
    }
}
