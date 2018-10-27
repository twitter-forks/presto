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

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;

import java.net.URI;

public class TwitterEventListenerConfig
{
    private String slackConfigFile;
    private String slackUsers;
    private HostAndPort slackHttpProxy;
    private URI slackUri;
    private String slackEmailTemplate;
    private String slackNotificationTemplateFile;
    private String knowledgeBaseFile;
    private String scribeCategory;

    public String getSlackConfigFile()
    {
        return slackConfigFile;
    }

    @Config("event-listener.slack-config-file")
    public TwitterEventListenerConfig setSlackConfigFile(String slackConfigFile)
    {
        this.slackConfigFile = slackConfigFile;
        return this;
    }

    public String getSlackUsers()
    {
        return slackUsers;
    }

    @Config("event-listener.slack-users")
    public TwitterEventListenerConfig setSlackUsers(String slackUsers)
    {
        this.slackUsers = slackUsers;
        return this;
    }

    public HostAndPort getSlackHttpProxy()
    {
        return slackHttpProxy;
    }

    @Config("event-listener.slack-http-proxy")
    public TwitterEventListenerConfig setSlackHttpProxy(HostAndPort slackHttpProxy)
    {
        this.slackHttpProxy = slackHttpProxy;
        return this;
    }

    public URI getSlackUri()
    {
        return slackUri;
    }

    @Config("event-listener.slack-uri")
    public TwitterEventListenerConfig setSlackUri(URI slackUri)
    {
        this.slackUri = slackUri;
        return this;
    }

    public String getSlackEmailTemplate()
    {
        return slackEmailTemplate;
    }

    @Config("event-listener.slack-email-template")
    public TwitterEventListenerConfig setSlackEmailTemplate(String slackEmailTemplate)
    {
        this.slackEmailTemplate = slackEmailTemplate;
        return this;
    }

    public String getSlackNotificationTemplateFile()
    {
        return slackNotificationTemplateFile;
    }

    @Config("event-listener.slack-notification-template-file")
    public TwitterEventListenerConfig setSlackNotificationTemplateFile(String slackNotificationTemplateFile)
    {
        this.slackNotificationTemplateFile = slackNotificationTemplateFile;
        return this;
    }

    public String getKnowledgeBaseFile()
    {
        return knowledgeBaseFile;
    }

    @Config("event-listener.knowledge-base-file")
    public TwitterEventListenerConfig setKnowledgeBaseFile(String knowledgeBaseFile)
    {
        this.knowledgeBaseFile = knowledgeBaseFile;
        return this;
    }

    public String getScribeCategory()
    {
        return scribeCategory;
    }

    @Config("event-listener.scribe-category")
    public TwitterEventListenerConfig setScribeCategory(String scribeCategory)
    {
        this.scribeCategory = scribeCategory;
        return this;
    }
}
