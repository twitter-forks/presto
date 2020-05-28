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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.ServiceLoader;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertNotNull;

public class TestTwitterEventListenerPlugin
{
    @Test
    public void testPlugin()
    {
        TwitterEventListenerPlugin plugin = loadPlugin(TwitterEventListenerPlugin.class);

        EventListenerFactory factory = getOnlyElement(plugin.getEventListenerFactories());
        assertInstanceOf(factory, TwitterEventListenerFactory.class);

        Map<String, String> config = ImmutableMap.of();

        EventListener eventListener = factory.create(config);
        assertNotNull(eventListener);
        assertInstanceOf(eventListener, TwitterEventListener.class);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Plugin> T loadPlugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("did not find plugin: " + clazz.getName());
    }
}
