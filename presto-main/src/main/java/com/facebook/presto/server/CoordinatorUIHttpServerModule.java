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

import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.Serialization.FunctionCallDeserializer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeDeserializer;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.AnnouncementHttpServerInfo;
import io.airlift.event.client.EventClient;
import io.airlift.http.server.HttpRequestEvent;
import io.airlift.http.server.HttpServer;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.HttpServerProvider;
import io.airlift.http.server.LocalAnnouncementHttpServerInfo;
import io.airlift.http.server.RequestStats;
import io.airlift.http.server.TheAdminServlet;
import io.airlift.http.server.TheServlet;
import io.airlift.slice.Slice;

import javax.servlet.Filter;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.event.client.EventBinder.eventBinder;
import static io.airlift.http.server.HttpServerBinder.HttpResourceBinding;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorUIHttpServerModule
        extends AbstractConfigurationAwareModule
{
    private final Injector injector;

    public CoordinatorUIHttpServerModule(Injector injector)
    {
        this.injector = requireNonNull(injector, "injector is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.disableCircularProxies();

        ServerConfig serverConfig = injector.getInstance(ServerConfig.class);
        HttpServerConfig httpServerConfig = injector.getInstance(HttpServerConfig.class)
                                          .setHttpPort(serverConfig.getUIHttpPort());
        binder.bind(HttpServerConfig.class).toInstance(httpServerConfig);

        binder.bind(HttpServerInfo.class).in(Scopes.SINGLETON);
        binder.bind(EventClient.class).toInstance(injector.getInstance(EventClient.class));
        binder.bind(HttpServer.class).toProvider(HttpServerProvider.class).in(Scopes.SINGLETON);

        Multibinder.newSetBinder(binder, Filter.class, TheServlet.class);
        Multibinder.newSetBinder(binder, Filter.class, TheAdminServlet.class);
        Multibinder.newSetBinder(binder, HttpResourceBinding.class, TheServlet.class);
        newExporter(binder).export(HttpServer.class).withGeneratedName();
        binder.bind(RequestStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RequestStats.class).withGeneratedName();
        eventBinder(binder).bindEventClient(HttpRequestEvent.class);

        binder.bind(AnnouncementHttpServerInfo.class).to(LocalAnnouncementHttpServerInfo.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).toInstance(injector.getInstance(BlockEncodingSerde.class));
        binder.bind(TypeManager.class).toInstance(injector.getInstance(TypeManager.class));
        binder.bind(SqlParser.class).toInstance(injector.getInstance(SqlParser.class));
        binder.bind(TransactionManager.class).toInstance(injector.getInstance(TransactionManager.class));

        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResults.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);
        jsonCodecBinder(binder).bindJsonCodec(MemoryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(MemoryPoolAssignmentsRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

        // bind webapp
        httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");
        // presto coordinator ui announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator-ui");
        // query execution visualizer
        jaxrsBinder(binder).bindInstance(injector.getInstance(QueryExecutionResource.class));
        // query manager
        jaxrsBinder(binder).bindInstance(injector.getInstance(QueryResource.class));
        jaxrsBinder(binder).bindInstance(injector.getInstance(StageResource.class));
        // cluster statistics
        jaxrsBinder(binder).bindInstance(injector.getInstance(ClusterStatsResource.class));
        // server info resource
        jaxrsBinder(binder).bindInstance(injector.getInstance(ServerInfoResource.class));
        // server node resource
        jaxrsBinder(binder).bindInstance(injector.getInstance(NodeResource.class));
    }
}
