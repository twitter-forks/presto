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
package com.twitter.presto.gateway.cluster;

import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Path("/v1/gateway/managers")
public class ClusterManagerResource
{
    private static final Logger log = Logger.get(ClusterStatusResource.class);

    private final ClusterManager clusterManager;

    @Inject
    public ClusterManagerResource(ClusterManager clusterManager)
    {
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
    }

    @GET
    public List<URI> getAllClusters()
    {
        return clusterManager.getAllClusters();
    }
}
