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

import com.facebook.presto.twitter.logging.QueryLogger;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;

public class ModuleLoader
{
    private ModuleLoader()
    {
    }

    public static Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of(binder ->
                Multibinder.newSetBinder(binder, EventClient.class)
                        .addBinding()
                        .to(QueryLogger.class)
                        .in(Scopes.SINGLETON));
    }
}
