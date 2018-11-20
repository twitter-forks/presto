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
package com.twitter.presto.plugin.eventlistener.slack;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@Path("/api")
public class SlackResource
{
    private Map<String, SlackImOpenResponse> imOpenResponses;
    private Map<Map.Entry<String, String>, SlackChatPostMessageResponse> chatPostMessageResponses;
    private Map<String, SlackUsersLookupByEmailResponse> usersLookupByEmailResponses;
    private Map<Map.Entry<String, String>, SlackImHistoryResponse> imHistoryResponses;

    private List<String> imOpenRequests = new ArrayList<>();
    private List<Map.Entry<String, String>> chatPostMessageRequests = new ArrayList<>();
    private List<String> usersLookupByEmailRequests = new ArrayList<>();
    private List<Map.Entry<String, String>> imHistoryRequests = new ArrayList<>();

    private CountDownLatch numCallsExpected;

    public void initialize(
            Map<String, SlackImOpenResponse> imOpenResponses,
            Map<Map.Entry<String, String>, SlackChatPostMessageResponse> chatPostMessageResponses,
            Map<String, SlackUsersLookupByEmailResponse> usersLookupByEmailResponses,
            Map<Map.Entry<String, String>, SlackImHistoryResponse> imHistoryResponses)
    {
        this.imOpenResponses = requireNonNull(imOpenResponses, "imOpenResponses is null");
        this.chatPostMessageResponses = requireNonNull(chatPostMessageResponses, "chatPostMessageResponses is null");
        this.usersLookupByEmailResponses = requireNonNull(usersLookupByEmailResponses, "usersLookupByEmailResponses is null");
        this.imHistoryResponses = requireNonNull(imHistoryResponses, "imHistoryResponses is null");
    }

    public void setNumCallsExpected(int numCallsExpected)
    {
        this.numCallsExpected = new CountDownLatch(numCallsExpected);
        imOpenRequests.clear();
        chatPostMessageRequests.clear();
        usersLookupByEmailRequests.clear();
        imHistoryRequests.clear();
    }

    public void waitForCalls(int timeoutSeconds)
            throws InterruptedException
    {
        numCallsExpected.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    public List<String> getImOpenRequests()
    {
        return imOpenRequests;
    }

    public List<Map.Entry<String, String>> getChatPostMessageRequests()
    {
        return chatPostMessageRequests;
    }

    public List<String> getUsersLookupByEmailRequests()
    {
        return usersLookupByEmailRequests;
    }

    public List<Map.Entry<String, String>> getImHistoryRequests()
    {
        return imHistoryRequests;
    }

    @Path("/im.open")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response ImOpen(SlackImOpenRequest request)
    {
        numCallsExpected.countDown();
        imOpenRequests.add(request.getUser());
        SlackImOpenResponse response = imOpenResponses.get(request.getUser());
        if (response == null) {
            response = imOpenResponses.get("*");
        }
        if (response == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        return Response.ok(response, MediaType.APPLICATION_JSON_TYPE).build();
    }

    @Path("/chat.postMessage")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response ChatPostMessage(SlackChatPostMessageRequest request)
    {
        numCallsExpected.countDown();
        Map.Entry<String, String> entry = new AbstractMap.SimpleImmutableEntry<>(request.getChannel(), request.getText());
        chatPostMessageRequests.add(entry);
        SlackChatPostMessageResponse response = chatPostMessageResponses.get(entry);
        if (response == null) {
            response = chatPostMessageResponses.get(new AbstractMap.SimpleImmutableEntry<>("*", "*"));
        }
        if (response == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        return Response.ok(response, MediaType.APPLICATION_JSON_TYPE).build();
    }

    @Path("/users.lookupByEmail")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response UsersLookupByEmail(@FormParam("email") String email)
    {
        numCallsExpected.countDown();
        usersLookupByEmailRequests.add(email);
        SlackUsersLookupByEmailResponse response = usersLookupByEmailResponses.get(email);
        if (response == null) {
            response = usersLookupByEmailResponses.get("*");
        }
        if (response == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        return Response.ok(response, MediaType.APPLICATION_JSON_TYPE).build();
    }

    @Path("/im.history")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response ImHistory(@FormParam("channel") String channel, @FormParam("latest") String latest)
    {
        numCallsExpected.countDown();
        Map.Entry<String, String> entry = new AbstractMap.SimpleImmutableEntry<>(channel, latest);
        imHistoryRequests.add(entry);
        SlackImHistoryResponse response = imHistoryResponses.get(entry);
        if (response == null) {
            response = imHistoryResponses.get(new AbstractMap.SimpleImmutableEntry<>("*", "*"));
        }
        if (response == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        return Response.ok(response, MediaType.APPLICATION_JSON_TYPE).build();
    }
}
