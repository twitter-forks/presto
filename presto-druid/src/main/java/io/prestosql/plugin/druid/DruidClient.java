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
package io.prestosql.plugin.druid;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.druid.metadata.DruidColumnInfo;
import io.prestosql.plugin.druid.metadata.DruidSegmentId;
import io.prestosql.plugin.druid.metadata.DruidTableInfo;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidClient
{
    // Druid API endpoints
    private static final String SQL_ENDPOINT = "/druid/v2/sql";

    private static final String LIST_TABLE_QUERY = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'";
    private static final String GET_COLUMN_TEMPLATE = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '%s'";
    private static final String GET_SEGMENTS_TEMPLATE = "SELECT segment_id FROM sys.segments WHERE datasource = '%s'";

    // codec
    private static final JsonCodec<List<DruidColumnInfo>> LIST_COLUMN_INFO_CODEC = listJsonCodec(DruidColumnInfo.class);
    private static final JsonCodec<List<DruidTableInfo>> LIST_TABLE_NAME_CODEC = listJsonCodec(DruidTableInfo.class);
    private static final JsonCodec<List<DruidSegmentId>> LIST_SEGMENT_ID_CODEC = listJsonCodec(DruidSegmentId.class);

    private final HttpClient httpClient;
    private final URI druidBroker;

    @Inject
    public DruidClient(DruidConfig config, @ForDruidClient HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.druidBroker = URI.create(config.getDruidBrokerUrl());
    }

    public List<String> getTables()
    {
        return httpClient.execute(prepareQuery(LIST_TABLE_QUERY), createJsonResponseHandler(LIST_TABLE_NAME_CODEC)).stream()
                .map(DruidTableInfo::getTableName)
                .collect(toImmutableList());
    }

    public List<DruidColumnInfo> getColumnDataType(String tableName)
    {
        return httpClient.execute(prepareQuery(format(GET_COLUMN_TEMPLATE, tableName)), createJsonResponseHandler(LIST_COLUMN_INFO_CODEC));
    }

    public List<String> getDataSegmentIds(String tableName)
    {
        return httpClient.execute(prepareQuery(format(GET_SEGMENTS_TEMPLATE, tableName)), createJsonResponseHandler(LIST_SEGMENT_ID_CODEC)).stream()
                .map(segmentId -> segmentId.getSegmentId())
                .collect(toImmutableList());
    }

    private static Request.Builder setContentTypeHeaders(Request.Builder requestBuilder)
    {
        return requestBuilder
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString());
    }

    private static byte[] createRequestBody(String query)
    {
        return format("{\"query\":\"%s\"}\n", query).getBytes();
    }

    private Request prepareQuery(String query)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(druidBroker).replacePath(SQL_ENDPOINT);

        return setContentTypeHeaders(preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(createRequestBody(query)))
                .build();
    }
}
