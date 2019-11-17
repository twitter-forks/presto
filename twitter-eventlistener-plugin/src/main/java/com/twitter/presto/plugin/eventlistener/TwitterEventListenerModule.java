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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.twitter.presto.plugin.eventlistener.scriber.QueryCompletedEventScriber;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class TwitterEventListenerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(TwitterEventListener.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TwitterEventListenerConfig.class);
        TwitterEventListenerConfig config = buildConfigObject(TwitterEventListenerConfig.class);
        Multibinder<TwitterEventHandler> twitterEventHandlerBinder = newSetBinder(binder, TwitterEventHandler.class);
        if (config.getScribeCategory() != null) {
            twitterEventHandlerBinder.addBinding().to(QueryCompletedEventScriber.class).in(Scopes.SINGLETON);
        }
    }
}
