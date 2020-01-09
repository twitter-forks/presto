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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.twitter.presto.gateway.GatewayConfig;
import com.twitter.presto.gateway.query.QueryCategory;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.twitter.presto.gateway.query.QueryCategory.BATCH;
import static com.twitter.presto.gateway.query.QueryCategory.INTERACTIVE;
import static com.twitter.presto.gateway.query.QueryCategory.REALTIME;

public class StaticClusterManager
        implements ClusterManager
{
    private final Map<QueryCategory, List<URI>> clusters;

    @Inject
    public StaticClusterManager(GatewayConfig config)
    {
        clusters = ImmutableMap.<QueryCategory, List<URI>>builder()
            .put(REALTIME, config.getClusters())
            .put(INTERACTIVE, config.getClusters())
            .put(BATCH, config.getClusters())
            .build();
    }

    @Override
    public List<URI> getAllClusters()
    {
        return clusters.values().stream()
            .flatMap(List::stream)
            .distinct()
            .collect(toImmutableList());
    }

    @Override
    public List<URI> getClusters(QueryCategory category)
    {
        return clusters.get(category);
    }
}
