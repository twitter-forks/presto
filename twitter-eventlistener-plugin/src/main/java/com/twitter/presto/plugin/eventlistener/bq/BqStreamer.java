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
package com.twitter.presto.plugin.eventlistener.bq;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Splitter;
import com.twitter.presto.plugin.eventlistener.TwitterEventHandler;
import com.twitter.presto.plugin.eventlistener.TwitterEventListenerConfig;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.twitter.presto.plugin.eventlistener.bq.MetadataHelper.mapToJson;
import static com.twitter.presto.plugin.eventlistener.bq.MetadataHelper.queryOutputMetadataToJson;
import static com.twitter.presto.plugin.eventlistener.bq.MetadataHelper.resourceEstimatesToJson;
import static java.util.stream.Collectors.toList;

public class BqStreamer
        implements TwitterEventHandler
{
    private static final Logger log = Logger.get(BqStreamer.class);

    private final BigQuery bigquery;
    private final TableId tableId;

    @Inject
    public BqStreamer(TwitterEventListenerConfig config)
    {
        List<String> parts = Splitter.on('.').splitToList(config.getBqTableFullName());
        checkArgument(parts.size() == 3, "invalid fully qualified bigquery table name");
        String projectId = parts.get(0);
        String datasetName = parts.get(1);
        String tableName = parts.get(2);

        this.bigquery = createInstance(config.getServiceAccountKeyFile());
        this.tableId = TableId.of(projectId, datasetName, tableName);
    }

    public void handleQueryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        Map<String, Object> rowContent = prepareRowContent(queryCompletedEvent);
        InsertAllResponse response =
                bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                                .addRow(rowContent)
                                .build());
        if (response.hasErrors()) {
            log.warn("failed to write QueryCompletedEvent to BigQuery: " +
                    response.getInsertErrors().entrySet().stream()
                            .map(x -> x.toString())
                            .collect(toList())
                            .toString());
        }
    }

    private static BigQuery createInstance(String jsonKeyPath)
    {
        if (jsonKeyPath == null) {
            // fall back to default Instance by looking up the VM metadata
            return BigQueryOptions.getDefaultInstance().getService();
        }

        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(new File(jsonKeyPath))) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }
        catch (IOException e) {
            throw new IllegalArgumentException();
        }

        return BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    private static Map<String, Object> prepareRowContent(QueryCompletedEvent queryCompletedEvent)
    {
        Map<String, Object> rowContent = new HashMap<>();
        // createTime, executionStartTime, endTime
        rowContent.put("create_time_ms", queryCompletedEvent.getCreateTime().toEpochMilli());
        rowContent.put("execution_start_time_ms", queryCompletedEvent.getExecutionStartTime().toEpochMilli());
        rowContent.put("end_time_ms", queryCompletedEvent.getEndTime().toEpochMilli());

        // QueryMetadata
        QueryMetadata queryMetadata = queryCompletedEvent.getMetadata();
        rowContent.put("query_id", queryMetadata.getQueryId());
        queryMetadata.getTransactionId().ifPresent(transactionId -> rowContent.put("transaction_id", transactionId));
        rowContent.put("query", queryMetadata.getQuery());
        rowContent.put("query_state", queryMetadata.getQueryState());
        rowContent.put("uri_", queryMetadata.getUri().toASCIIString());
        queryMetadata.getPlan().ifPresent(plan -> rowContent.put("plan", plan));
        queryMetadata.getPayload().ifPresent(payload -> rowContent.put("payload", payload));

        // QueryStatistics
        QueryStatistics queryStatistics = queryCompletedEvent.getStatistics();
        rowContent.put("cpu_time_ms", queryStatistics.getCpuTime().toMillis());
        rowContent.put("wall_time_ms", queryStatistics.getWallTime().toMillis());
        rowContent.put("queued_time_ms", queryStatistics.getQueuedTime().toMillis());
        queryStatistics.getAnalysisTime().ifPresent(analysisTime -> rowContent.put("analysis_time_ms", analysisTime.toMillis()));
        queryStatistics.getDistributedPlanningTime().ifPresent(planingTime -> rowContent.put("distributed_planning_time_ms", planingTime.toMillis()));
        rowContent.put("peak_user_memory_bytes", queryStatistics.getPeakTotalNonRevocableMemoryBytes());
        rowContent.put("peak_total_non_revocable_memory_bytes", queryStatistics.getPeakTotalNonRevocableMemoryBytes());
        rowContent.put("peak_task_total_memory", queryStatistics.peakTaskTotalMemory());
        rowContent.put("total_bytes", queryStatistics.getTotalBytes());
        rowContent.put("total_rows", queryStatistics.getTotalRows());
        rowContent.put("output_bytes", queryStatistics.getOutputBytes());
        rowContent.put("output_rows", queryStatistics.getOutputRows());
        rowContent.put("written_bytes", queryStatistics.getWrittenBytes());
        rowContent.put("written_rows", queryStatistics.getWrittenRows());
        rowContent.put("cumulative_memory_bytesecond", queryStatistics.getCumulativeMemory());
        rowContent.put("stage_gc_statistics", queryStatistics.getStageGcStatistics().stream().map(MetadataHelper::stageGcStatisticsToJson).collect(toList()));
        rowContent.put("splits", queryStatistics.getCompletedSplits());
        rowContent.put("complete", queryStatistics.isComplete());
        rowContent.put("cpu_time_distribution", queryStatistics.getCpuTimeDistribution().stream().map(MetadataHelper::stageCpuDistributionToJson).collect(toList()));
        rowContent.put("operator_summaries", queryStatistics.getOperatorSummaries());

        // QueryContext
        QueryContext queryContext = queryCompletedEvent.getContext();
        rowContent.put("user_", queryContext.getUser());
        queryContext.getPrincipal().ifPresent(principal -> rowContent.put("principal", principal));
        queryContext.getRemoteClientAddress().ifPresent(remoteClientAddress -> rowContent.put("remote_client_address", remoteClientAddress));
        queryContext.getUserAgent().ifPresent(userAgent -> rowContent.put("user_gent", userAgent));
        queryContext.getClientInfo().ifPresent(clientInfo -> rowContent.put("client_info", clientInfo));
        rowContent.put("client_tags", queryContext.getClientTags().stream().collect(toList()));
        rowContent.put("client_capabilities", queryContext.getClientCapabilities().stream().collect(toList()));
        queryContext.getSource().ifPresent(source -> rowContent.put("source", source));
        queryContext.getCatalog().ifPresent(catalog -> rowContent.put("catalog", catalog));
        queryContext.getSchema().ifPresent(schema -> rowContent.put("schema_", schema));
        queryContext.getResourceGroupId().ifPresent(segments -> rowContent.put("resource_group_id_segments", segments.getSegments()));
        rowContent.put("session_properties_json", mapToJson(queryContext.getSessionProperties()));
        rowContent.put("resource_estimates_json", resourceEstimatesToJson(queryContext.getResourceEstimates()));
        rowContent.put("server_address", queryContext.getServerAddress());
        rowContent.put("server_version", queryContext.getServerVersion());
        rowContent.put("environment", queryContext.getEnvironment());

        // QueryIOMetadata
        QueryIOMetadata queryIOMetadata = queryCompletedEvent.getIoMetadata();
        rowContent.put("io_inputs_metadata_json", queryIOMetadata.getInputs().stream().map(MetadataHelper::queryInputMetadataToJson).collect(toList()));
        queryIOMetadata.getOutput().ifPresent(metadata -> rowContent.put("io_output_metadata_json", queryOutputMetadataToJson(metadata)));

        // QueryFailureInfo
        queryCompletedEvent.getFailureInfo().ifPresent(
                failureInfo -> {
                    rowContent.put("error_code_id", failureInfo.getErrorCode().getCode());
                    rowContent.put("error_code_name", failureInfo.getErrorCode().getName());
                    rowContent.put("error_code_type", failureInfo.getErrorCode().getType().toString());
                    failureInfo.getFailureType().ifPresent(failureType -> rowContent.put("failure_type", failureType));
                    failureInfo.getFailureMessage().ifPresent(failureMessage -> rowContent.put("failure_message", failureMessage));
                    failureInfo.getFailureTask().ifPresent(failureTask -> rowContent.put("failure_task", failureTask));
                    failureInfo.getFailureHost().ifPresent(failureHost -> rowContent.put("failure_host", failureHost));
                    rowContent.put("failures_json", failureInfo.getFailuresJson());
                });

        return rowContent;
    }
}
