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
package com.twitter.presto.maintenance;

import com.facebook.presto.spi.NodeState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.json.JSONObject;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

import java.net.URI;
import java.util.Optional;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

@Path("/canDrain")
public class MaintenanceCoordinatorResource
{
    private static final Logger log = Logger.get(MaintenanceCoordinatorResource.class);
    private static final JsonCodec<NodeState> NODE_STATE_CODEC = jsonCodec(NodeState.class);

    private final HttpClient httpClient;

    @Inject
    public MaintenanceCoordinatorResource(@ForMaintenance HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @POST
    public DrainResponse canDrain(String jsonString)
    {
        URI nodeUri = extractHostUri(jsonString);
        log.info("Try draining node : " + nodeUri);

        // check the state of the target node
        Optional<NodeState> state = getNodeState(nodeUri);

        if (state.isPresent()) {
            // if the node is active, we send the shutdown request
            if (state.get() == NodeState.ACTIVE) {
                shutdownNode(nodeUri);
            }
            return new DrainResponse(false);
        }

        // By design we should NEVER return true to drain request. What will happen is that the first request will request graceful shutdown in the target and the target node
        // state will transfer from ACTIVE to SHUTTING_DOWN. When the shutdown is completed, getNodeState() will fail and the exception will propagate to aurora COp.
        // COp always list active tasks before requesting drain, but there is a race condition which may expose a small window where the task finishes between COp list the
        // active tasks and maintenance coordinator query the state of the target. COp will treat the exception as a NO, and the next retry should proceed without requesting
        // maintenance coordinator.
        return new DrainResponse(true);
    }

    private Optional<NodeState> getNodeState(URI nodeUri)
    {
        URI stateInfoUri = uriBuilderFrom(nodeUri).appendPath("/v1/info/state").build();
        // synchronously send SHUTTING_DOWN request to worker node
        Request request = prepareGet()
                .setUri(stateInfoUri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        JsonResponse<NodeState> response = httpClient.execute(request, createFullJsonResponseHandler(NODE_STATE_CODEC));

        log.info("Node " + nodeUri + " in state : " + response.getValue());
        return Optional.of(response.getValue());
    }

    private void shutdownNode(URI nodeUri)
    {
        log.info("Shutting down node : " + nodeUri);
        URI stateInfoUri = uriBuilderFrom(nodeUri).appendPath("/v1/info/state").build();
        Request request = preparePut()
                .setUri(stateInfoUri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(NodeState.class), NodeState.SHUTTING_DOWN))
                .build();

        httpClient.execute(request, createStatusResponseHandler());
    }

    // extract the worker node URI from the request body
    private URI extractHostUri(String message)
    {
        JSONObject jsonBody = new JSONObject(message);
        String hostName = jsonBody
                .getJSONObject("taskConfig")
                .getJSONObject("assignedTask")
                .get("slaveHost")
                .toString();
        int port = (Integer) jsonBody
                .getJSONObject("taskConfig")
                .getJSONObject("assignedTask")
                .getJSONObject("assignedPorts")
                .get("http");
        return URI.create("http://" + hostName + ":" + port);
    }

    public static class DrainResponse
    {
        private final boolean drain;

        @JsonCreator
        public DrainResponse(@JsonProperty("drain") boolean drain)
        {
            this.drain = drain;
        }

        @JsonProperty
        public boolean getDrain()
        {
            return drain;
        }
    }
}
