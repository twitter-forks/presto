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
package com.facebook.presto.twitter;

import com.facebook.presto.event.EventProcessor;
import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.event.query.QueryCreatedEvent;
import com.facebook.presto.event.query.QueryEventHandler;
import com.facebook.presto.event.query.SplitCompletionEvent;
import com.facebook.presto.twitter.logging.QueryLogger;
import com.facebook.presto.twitter.logging.QueryScriber;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;

/**
 * Loader that initializes custom Twitter code to inject into Presto. Whenever
 * possible we should use this pattern to inject custom functionality, since it
 * makes it easier to differentiate our patches from the core OS code.
 *
 * If the functionality we wish to add/override isn't currently possible to via
 * overriding a guice module, we should contribute the necessary modules/interfaces
 * into the OS Presto code base to make it possible.
 */
public class TwitterModuleLoader
{
    private TwitterModuleLoader()
    {
    }

    public static Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of(
                binder -> Multibinder.newSetBinder(binder, EventClient.class)
                        .addBinding()
                        .to(EventProcessor.class)
                        .in(Scopes.SINGLETON),
                binder -> Multibinder.newSetBinder(binder, new TypeLiteral<QueryEventHandler<SplitCompletionEvent>>() {}),
                binder -> Multibinder.newSetBinder(binder, new TypeLiteral<QueryEventHandler<QueryCreatedEvent>>() {}),
                binder -> Multibinder.newSetBinder(binder, new TypeLiteral<QueryEventHandler<QueryCompletionEvent>>(){})
                        .addBinding()
                        .to(new TypeLiteral<QueryLogger>(){})
                        .in(Scopes.SINGLETON),
                binder -> Multibinder.newSetBinder(binder, new TypeLiteral<QueryEventHandler<QueryCompletionEvent>>(){})
                        .addBinding()
                        .to(QueryScriber.class)
                        .in(Scopes.SINGLETON)
                );
    }
}
