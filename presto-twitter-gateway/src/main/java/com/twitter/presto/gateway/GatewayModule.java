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
package com.twitter.presto.gateway;

import com.facebook.presto.client.NodeVersion;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.twitter.presto.gateway.cluster.ClusterManager;
import com.twitter.presto.gateway.cluster.ClusterManagerResource;
import com.twitter.presto.gateway.cluster.ClusterSelector;
import com.twitter.presto.gateway.cluster.ClusterStatusResource;
import com.twitter.presto.gateway.cluster.ClusterStatusTracker;
import com.twitter.presto.gateway.cluster.ForQueryTracker;
import com.twitter.presto.gateway.cluster.RandomSelector;
import com.twitter.presto.gateway.cluster.StaticClusterManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;

import static com.twitter.presto.gateway.GatewayConfig.ClusterManagerType;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GatewayModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GatewayConfig.class);
        binder.bind(ClusterSelector.class).to(RandomSelector.class).in(Scopes.SINGLETON);

        GatewayConfig gatewayConfig = buildConfigObject(GatewayConfig.class);
        ClusterManagerType type = buildConfigObject(GatewayConfig.class).getClusterManagerType();
        switch (type) {
            case STATIC:
                binder.bind(ClusterManager.class).to(StaticClusterManager.class).in(Scopes.SINGLETON);
                break;
            default:
                throw new AssertionError("Unsupported cluster manager type: " + type);
        }

        httpClientBinder(binder).bindHttpClient("query-tracker", ForQueryTracker.class)
                        .withConfigDefaults(config -> {
                            config.setIdleTimeout(new Duration(30, SECONDS));
                            config.setRequestTimeout(new Duration(10, SECONDS));
                        });

        NodeVersion nodeVersion = new NodeVersion(gatewayConfig.getVersion());
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        binder.bind(ClusterStatusTracker.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(GatewayResource.class);
        jaxrsBinder(binder).bind(ClusterManagerResource.class);
        jaxrsBinder(binder).bind(ClusterStatusResource.class);
    }
}
