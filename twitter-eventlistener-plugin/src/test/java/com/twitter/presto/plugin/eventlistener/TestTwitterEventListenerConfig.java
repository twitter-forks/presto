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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

public class TestTwitterEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(TwitterEventListenerConfig.class)
                .setScribeCategory(null)
                .setSlackConfigFile(null)
                .setSlackEmailTemplate(null)
                .setSlackHttpProxy(null)
                .setSlackNotificationTemplateFile(null)
                .setKnowledgeBaseFile(null)
                .setSlackUri(null)
                .setSlackUsers(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("event-listener.scribe-category", "test")
                .put("event-listener.knowledge-base-file", "/etc/config/knowledge.json")
                .put("event-listener.slack-config-file", "/etc/config/slack.json")
                .put("event-listener.slack-email-template", "${USER}@domain.top")
                .put("event-listener.slack-http-proxy", "localhost:1008")
                .put("event-listener.slack-notification-template-file", "/etc/config/notification.json")
                .put("event-listener.slack-uri", "https://slack.com")
                .put("event-listener.slack-users", "user1|user2")
                .build();

        TwitterEventListenerConfig expected = new TwitterEventListenerConfig()
                .setScribeCategory("test")
                .setKnowledgeBaseFile("/etc/config/knowledge.json")
                .setSlackConfigFile("/etc/config/slack.json")
                .setSlackEmailTemplate("${USER}@domain.top")
                .setSlackHttpProxy(HostAndPort.fromString("localhost:1008"))
                .setSlackNotificationTemplateFile("/etc/config/notification.json")
                .setSlackUri(URI.create("https://slack.com"))
                .setSlackUsers("user1|user2");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
