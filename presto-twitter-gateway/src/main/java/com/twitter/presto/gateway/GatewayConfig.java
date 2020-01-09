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

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;

public class GatewayConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private String version = getClass().getPackage().getImplementationVersion();
    private ClusterManagerType clusterManagerType;
    private List<URI> clusters;

    public enum ClusterManagerType
    {
        STATIC
    }

    @NotNull(message = "Gateway version cannot be automatically determined")
    public String getVersion()
    {
        return version;
    }

    @Config("gateway.version")
    public GatewayConfig setVersion(String version)
    {
        this.version = version;
        return this;
    }

    @NotNull
    public ClusterManagerType getClusterManagerType()
    {
        return clusterManagerType;
    }

    @Config("gateway.cluster-manager.type")
    @ConfigDescription("Cluster manager type (supported types: STATIC)")
    public GatewayConfig setClusterManagerType(String type)
    {
        if (type == null) {
            clusterManagerType = null;
            return this;
        }

        clusterManagerType = ClusterManagerType.valueOf(type);
        return this;
    }

    @NotNull
    public List<URI> getClusters()
    {
        return clusters;
    }

    @Config("gateway.cluster-manager.static.cluster-list")
    @ConfigDescription("List of Presto clusters behind the gateway")
    public GatewayConfig setClusters(String clusterList)
    {
        if (clusterList == null) {
            this.clusters = null;
            return this;
        }
        this.clusters = stream(SPLITTER.split(clusterList))
            .map(URI::create)
            .collect(toImmutableList());
        return this;
    }
}
