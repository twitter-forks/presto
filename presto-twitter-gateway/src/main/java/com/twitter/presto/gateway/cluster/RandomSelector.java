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

import com.twitter.presto.gateway.RequestInfo;
import com.twitter.presto.gateway.query.QueryCategory;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.twitter.presto.gateway.query.QueryClassifier.classify;
import static java.util.Objects.requireNonNull;

public class RandomSelector
        implements ClusterSelector
{
    private static final Random random = new Random();

    private final ClusterManager clusterManager;

    @Inject
    public RandomSelector(ClusterManager clusterManager)
    {
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
    }

    @Override
    public Optional<URI> getPrestoCluster(RequestInfo request)
    {
        QueryCategory category = classify(request);
        List<URI> candidates = clusterManager.getClusters(category);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(candidates.get(random.nextInt(candidates.size())));
    }
}
