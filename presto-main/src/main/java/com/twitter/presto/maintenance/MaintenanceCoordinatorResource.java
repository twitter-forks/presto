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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.NodeState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePut;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

@Path("/canDrain")
public class MaintenanceCoordinatorResource
{
    private static final Logger log = Logger.get(MaintenanceCoordinatorResource.class);
    private static final JsonCodec<NodeState> NODE_STATE_CODEC = jsonCodec(NodeState.class);
    private static final ObjectMapper jsonObjectMapper = new ObjectMapper();
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
        NodeState state = getNodeState(nodeUri);

        // if the node is active, we send the shutdown request
        if (state == NodeState.ACTIVE) {
            shutdownNode(nodeUri);
        }
        return new DrainResponse(false);

        // We should NEVER return "true" to drain request. What will happen is that the first request will request graceful shutdown in the target and the target node
        // state will transfer from ACTIVE to SHUTTING_DOWN. When the shutdown is completed, getNodeState() will fail and the exception will propagate to aurora COp.
        // COp always list active tasks before requesting drain, but there is a race condition which may expose a small window where the task finishes between COp list the
        // active tasks and maintenance coordinator query the state of the target. COp will treat the exception as a NO, and the next retry should proceed without requesting
        // maintenance coordinator.
    }

    private NodeState getNodeState(URI nodeUri)
    {
        // synchronously send SHUTTING_DOWN request to worker node
        Request request = prepareGet()
                .setUri(getNodeStateUri(nodeUri))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        NodeState nodeState = httpClient.execute(request, createJsonResponseHandler(NODE_STATE_CODEC));

        log.info("Node " + nodeUri + " in state : " + nodeState);
        return nodeState;
    }

    private void shutdownNode(URI nodeUri)
    {
        log.info("Shutting down node : " + nodeUri);
        Request request = preparePut()
                .setUri(getNodeStateUri(nodeUri))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(NodeState.class), NodeState.SHUTTING_DOWN))
                .build();

        httpClient.execute(request, createStatusResponseHandler());
    }

    // extract the worker node URI from the request body
    private URI extractHostUri(String message)
    {
        try {
            JsonNode jsonRoot = jsonObjectMapper.readTree(message);
            String hostName = jsonRoot
                    .get("taskConfig")
                    .get("assignedTask")
                    .get("slaveHost")
                    .asText();
            int port = jsonRoot
                    .get("taskConfig")
                    .get("assignedTask")
                    .get("assignedPorts")
                    .get("http")
                    .asInt();
            return URI.create("http://" + hostName + ":" + port);
        }
        catch (IOException e) {
            String errorMessage = "Malformed Json body in drain request " + message;
            log.warn(e, errorMessage);
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST)
                            .type(TEXT_PLAIN_TYPE)
                            .entity(errorMessage)
                            .build());
        }
    }

    private URI getNodeStateUri(URI nodeUri)
    {
        return uriBuilderFrom(nodeUri).appendPath("/v1/info/state").build();
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
