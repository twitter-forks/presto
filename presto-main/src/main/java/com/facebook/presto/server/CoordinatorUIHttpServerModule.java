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
package com.facebook.presto.server;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.AnnouncementHttpServerInfo;
import io.airlift.http.server.HttpRequestEvent;
import io.airlift.http.server.HttpServer;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.HttpServerProvider;
import io.airlift.http.server.LocalAnnouncementHttpServerInfo;
import io.airlift.http.server.RequestStats;
import io.airlift.http.server.TheAdminServlet;
import io.airlift.http.server.TheServlet;

import javax.servlet.Filter;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.event.client.EventBinder.eventBinder;
import static io.airlift.http.server.HttpServerBinder.HttpResourceBinding;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorUIHttpServerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        if (serverConfig.isCoordinator()) {
            HttpServerConfig config = new HttpServerConfig().setHttpPort(serverConfig.getUIHttpPort());
            binder.bind(HttpServerConfig.class).toInstance(config);
            // HttpServerConfig config = buildConfigObject(HttpServerConfig.class);
            // config.setHttpPort(serverConfig.getUIHttpPort());

            binder.bind(HttpServerInfo.class).in(Scopes.SINGLETON);
            binder.bind(HttpServer.class).toProvider(HttpServerProvider.class).in(Scopes.SINGLETON);
            binder.disableCircularProxies();

            Multibinder.newSetBinder(binder, Filter.class, TheServlet.class);
            Multibinder.newSetBinder(binder, Filter.class, TheAdminServlet.class);
            Multibinder.newSetBinder(binder, HttpResourceBinding.class, TheServlet.class);
            newExporter(binder).export(HttpServer.class).withGeneratedName();
            binder.bind(RequestStats.class).in(Scopes.SINGLETON);
            newExporter(binder).export(RequestStats.class).withGeneratedName();
            eventBinder(binder).bindEventClient(HttpRequestEvent.class);

            binder.bind(AnnouncementHttpServerInfo.class).to(LocalAnnouncementHttpServerInfo.class).in(Scopes.SINGLETON);

            httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");

            // presto coordinator announcement
            discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator-ui");
        }
    }
}
