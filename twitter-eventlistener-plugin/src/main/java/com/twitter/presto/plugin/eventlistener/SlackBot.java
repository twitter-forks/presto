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
package com.twitter.presto.plugin.eventlistener;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import com.twitter.presto.plugin.eventlistener.slack.SlackChannel;
import com.twitter.presto.plugin.eventlistener.slack.SlackChatPostMessageRequest;
import com.twitter.presto.plugin.eventlistener.slack.SlackChatPostMessageResponse;
import com.twitter.presto.plugin.eventlistener.slack.SlackImHistoryResponse;
import com.twitter.presto.plugin.eventlistener.slack.SlackImOpenRequest;
import com.twitter.presto.plugin.eventlistener.slack.SlackImOpenResponse;
import com.twitter.presto.plugin.eventlistener.slack.SlackMessage;
import com.twitter.presto.plugin.eventlistener.slack.SlackResponse;
import com.twitter.presto.plugin.eventlistener.slack.SlackUsersLookupByEmailResponse;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import okhttp3.Authenticator;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import javax.inject.Inject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.PROXY_AUTHORIZATION;
import static java.lang.String.format;
import static java.net.Proxy.Type.HTTP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class SlackBot
        implements TwitterEventHandler
{
    private static final MediaType JSON_CONTENT_TYPE = MediaType.parse("Content-type: application/json; charset=utf-8");
    private static final String USER = "\\$\\{USER}";
    private static final String QUERY_ID = "\\$\\{QUERY_ID}";
    private static final String PRINCIPAL = "\\$\\{PRINCIPAL}";
    private static final String STATE = "\\$\\{STATE}";
    private static final String DASH = "-";
    private static final String CREATED = "created";
    private static final String COMPLETED = "completed";
    private static final String STOP = "stop";
    private static final String STOP_PRINCIPAL = "stop principal=%s";
    private static final String STOP_EVENT = "stop event=%s";
    private static final String STOP_STATE = "stop state=%s";
    private static final String RESUME = "resume";
    private static final String RESUME_PRINCIPAL = "resume principal=%s";
    private static final String RESUME_EVENT = "resume event=%s";
    private static final String RESUME_STATE = "resume state=%s";
    private final Logger log = Logger.get(SlackBot.class);
    private final SlackBotCredentials slackBotCredentials;
    private final Pattern slackUsers;
    private final URI slackUri;
    private final String emailTemplate;
    private final String notificationTemplateQueryCreated;
    private final String notificationTemplateQueryCompleted;
    private final OkHttpClient client;

    @Inject
    public SlackBot(TwitterEventListenerConfig config)
            throws IOException
    {
        requireNonNull(config.getSlackConfigFile(), "slack config file is null");
        this.slackBotCredentials = parse(Files.readAllBytes(Paths.get(config.getSlackConfigFile())), SlackBotCredentials.class);
        this.slackUsers = Pattern.compile(requireNonNull(config.getSlackUsers()));
        this.slackUri = requireNonNull(config.getSlackUri());
        this.emailTemplate = requireNonNull(config.getSlackEmailTemplate());
        this.notificationTemplateQueryCreated = config.getSlackNotificationTemplateQueryCreated();
        this.notificationTemplateQueryCompleted = config.getSlackNotificationTemplateQueryCompleted();

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        if (slackBotCredentials.getProxyUser().isPresent() && slackBotCredentials.getProxyPassword().isPresent() && config.getSlackHttpProxy() != null) {
            setupHttpProxy(builder, Optional.of(config.getSlackHttpProxy()));
            builder.proxyAuthenticator(basicAuth(PROXY_AUTHORIZATION, slackBotCredentials.getProxyUser().get(), slackBotCredentials.getProxyPassword().get()));
        }

        this.client = builder.build();
    }

    @Override
    public void handleQueryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        handleSlackNotification(CREATED,
                queryCreatedEvent.getContext().getUser(),
                queryCreatedEvent.getMetadata().getQueryId(),
                queryCreatedEvent.getContext().getPrincipal(),
                queryCreatedEvent.getMetadata().getQueryState());
    }

    @Override
    public void handleQueryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        handleSlackNotification(COMPLETED,
                queryCompletedEvent.getContext().getUser(),
                queryCompletedEvent.getMetadata().getQueryId(),
                queryCompletedEvent.getContext().getPrincipal(),
                queryCompletedEvent.getMetadata().getQueryState());
    }

    private void handleSlackNotification(String event, String user, String queryId, Optional<String> principal, String state)
    {
        if (!slackUsers.matcher(user).matches()) {
            return;
        }
        String template;
        switch (event) {
            case CREATED:
                template = notificationTemplateQueryCreated;
                break;
            case COMPLETED:
                template = notificationTemplateQueryCompleted;
                break;
            default:
                return;
        }
        try {
            String email = emailTemplate.replaceAll(USER, user);
            String text = template
                    .replaceAll(QUERY_ID, queryId)
                    .replaceAll(STATE, state)
                    .replaceAll(PRINCIPAL, principal.orElse(DASH));
            Consumer<String> sender = userLookupByEmail(openChannel(slackImOpenResponse -> {
                shouldSend(slackImOpenResponse, Optional.empty(), event, principal, state, postMessage(text, slackChatPostMessageResponse -> {
                    log.debug(format("sent the following message to user %s:\n%s\n", user, slackChatPostMessageResponse.getMessage().map(SlackMessage::getText).orElse("unknown")));
                }));
            }));
            sender.accept(email);
        }
        catch (Exception e) {
            log.warn(e, "Failed to send the slack notification");
        }
    }

    private Consumer<String> userLookupByEmail(Consumer<SlackUsersLookupByEmailResponse> next)
    {
        return email -> {
            FormBody body = new FormBody.Builder(UTF_8)
                    .add("email", email)
                    .build();
            postForm("/api/users.lookupByEmail",
                    body,
                    SlackUsersLookupByEmailResponse.class,
                    next);
        };
    }

    private Consumer<SlackUsersLookupByEmailResponse> openChannel(Consumer<SlackImOpenResponse> next)
    {
        return slackUsersLookupByEmailResponse -> {
            String userId = slackUsersLookupByEmailResponse.getUser().orElseThrow(() -> new RuntimeException("Failed to get user info")).getId();
            postJson("/api/im.open",
                    encode(new SlackImOpenRequest(userId), SlackImOpenRequest.class),
                    SlackImOpenResponse.class,
                    next);
        };
    }

    private void shouldSend(SlackImOpenResponse response, Optional<String> latest, String event, Optional<String> principal, String state, Consumer<SlackImOpenResponse> postMessage)
    {
        SlackChannel channel = response.getChannel().orElseThrow(() -> new RuntimeException("Failed to open the user channel"));
        Consumer<Optional<String>> checker = getChannelHistory(channel.getId(), history -> {
            Optional<String> newLatest = latest;
            if (!history.getMessages().isPresent()) {
                postMessage.accept(response);
                return;
            }
            for (SlackMessage message : history.getMessages().get()) {
                Optional<Boolean> result = shouldSend(message, event, principal, state);
                if (result.isPresent()) {
                    if (result.get()) {
                        postMessage.accept(response);
                    }
                    return;
                }
                if (!newLatest.isPresent() || Double.valueOf(newLatest.get()) > Double.valueOf(message.getTs())) {
                    newLatest = Optional.of(message.getTs());
                }
            }
            if (!history.getHasMore().isPresent() || !history.getHasMore().get()) {
                postMessage.accept(response);
                return;
            }
            shouldSend(response, newLatest, event, principal, state, postMessage);
        });
        checker.accept(latest);
    }

    private Optional<Boolean> shouldSend(SlackMessage message, String event, Optional<String> principal, String state)
    {
        String text = message.getText().trim();
        if (message.getText().trim().equalsIgnoreCase(RESUME)) {
            return Optional.of(true);
        }
        if (principal.isPresent() && text.equalsIgnoreCase(format(RESUME_PRINCIPAL, principal.get()))) {
            return Optional.of(true);
        }
        if (text.equalsIgnoreCase(format(RESUME_EVENT, event))) {
            return Optional.of(true);
        }
        if (text.equalsIgnoreCase(format(RESUME_STATE, state))) {
            return Optional.of(true);
        }
        if (text.equalsIgnoreCase(STOP)) {
            return Optional.of(false);
        }
        if (principal.isPresent() && text.equalsIgnoreCase(format(STOP_PRINCIPAL, principal.get()))) {
            return Optional.of(false);
        }
        if (text.equalsIgnoreCase(format(STOP_EVENT, event))) {
            return Optional.of(false);
        }
        if (text.equalsIgnoreCase(format(STOP_STATE, state))) {
            return Optional.of(false);
        }

        return Optional.empty();
    }

    private Consumer<Optional<String>> getChannelHistory(String channel, Consumer<SlackImHistoryResponse> next)
    {
        return latest -> {
            FormBody.Builder body = new FormBody.Builder(UTF_8)
                    .add("channel", channel);
            latest.ifPresent(ts -> body.add("latest", ts));
            postForm("/api/im.history",
                    body.build(),
                    SlackImHistoryResponse.class,
                    next);
        };
    }

    private Consumer<SlackImOpenResponse> postMessage(String text, Consumer<SlackChatPostMessageResponse> next)
    {
        return slackImOpenResponse -> {
            String channel = slackImOpenResponse.getChannel().orElseThrow(() -> new RuntimeException("Failed to open the user channel")).getId();
            postJson("/api/chat.postMessage",
                    encode(new SlackChatPostMessageRequest(channel, text), SlackChatPostMessageRequest.class),
                    SlackChatPostMessageResponse.class,
                    next);
        };
    }

    private <R extends RequestBody, T extends SlackResponse> void postForm(String path, R body, Class<T> javaType, Consumer<T> next)
    {
        String type = "application/x-www-form-urlencoded; charset=utf-8";
        post(path, type, body, javaType, next);
    }

    private <R extends RequestBody, T extends SlackResponse> void postJson(String path, R body, Class<T> javaType, Consumer<T> next)
    {
        String type = "application/json; charset=utf-8";
        post(path, type, body, javaType, next);
    }

    private <T extends SlackResponse> void post(String path, String type, RequestBody body, Class<T> javaType, Consumer<T> next)
    {
        HttpUrl url = HttpUrl.get(URI.create(slackUri.toString() + path));

        Request request = new Request.Builder()
                .url(requireNonNull(url))
                .header(CONTENT_TYPE, type)
                .header(AUTHORIZATION, "Bearer " + slackBotCredentials.getToken())
                .post(body)
                .build();
        client.newCall(request).enqueue(new Callback()
        {
            @Override
            public void onFailure(Call call, IOException e)
            {
                log.warn(e, "Failed to send the slack notification");
            }

            @Override
            public void onResponse(Call call, Response response)
                    throws IOException
            {
                requireNonNull(response.body(), "response.body() is null");
                T content = parse(response.body().bytes(), javaType);
                if (!content.isOk()) {
                    throw new RuntimeException(format("Slack responded an error message: %s", content.getError().orElse("unknown")));
                }
                next.accept(content);
            }
        });
    }

    private static void setupHttpProxy(OkHttpClient.Builder clientBuilder, Optional<HostAndPort> httpProxy)
    {
        setupProxy(clientBuilder, httpProxy, HTTP);
    }

    private static void setupProxy(OkHttpClient.Builder clientBuilder, Optional<HostAndPort> proxy, Proxy.Type type)
    {
        proxy.map(SlackBot::toUnresolvedAddress)
                .map(address -> new Proxy(type, address))
                .ifPresent(clientBuilder::proxy);
    }

    private static InetSocketAddress toUnresolvedAddress(HostAndPort address)
    {
        return InetSocketAddress.createUnresolved(address.getHost(), address.getPort());
    }

    public static Authenticator basicAuth(String scope, String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        if (user.contains(":")) {
            throw new RuntimeException("Illegal character ':' found in username");
        }

        return createAuthenticator(scope, Credentials.basic(user, password));
    }

    private static Authenticator createAuthenticator(String scope, String credential)
    {
        return (route, response) -> {
            if (response.request().header(scope) != null) {
                return null; // Give up, we've already failed to authenticate.
            }

            return response.request().newBuilder()
                    .header(scope, credential)
                    .build();
        };
    }

    private static <T> T parse(byte[] json, Class<T> javaType)
    {
        ObjectMapper mapper = new ObjectMapperProvider().get()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        try {
            return mapper.readValue(json, javaType);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Invalid JSON string for %s", javaType), e);
        }
    }

    private static <T> RequestBody encode(Object json, Class<T> javaType)
    {
        ObjectMapper mapper = new ObjectMapperProvider().get()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        try {
            String data = mapper.writerFor(javaType).writeValueAsString(json);
            return RequestBody.create(JSON_CONTENT_TYPE, data);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Invalid JSON string for %s", javaType), e);
        }
    }
}
