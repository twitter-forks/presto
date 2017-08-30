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
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;

import com.twitter.presto.thriftjava.QueryCompletionEvent;
import com.twitter.presto.thriftjava.QueryState;

import io.airlift.log.Logger;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class that scribes query completion events
 */
public class QueryCompletedEventScriber
{
  private static final String DASH = "-";
  private static final Logger log = Logger.get(QueryCompletedEventScriber.class);

  private TwitterScriber scriber = new TwitterScriber("presto_query_completion");

  public void handle(QueryCompletedEvent event)
  {
    try {
      scriber.scribe(toThriftQueryCompletionEvent(event));
    }
    catch (TException e) {
      log.warn(e,
        String.format("Could not serialize thrift object of Query(id=%s, user=%s, env=%s, schema=%s.%s)",
          event.getMetadata().getQueryId(),
          event.getContext().getUser(),
          event.getContext().getEnvironment(),
          event.getContext().getCatalog().orElse(DASH),
          event.getContext().getSchema().orElse(DASH)));
    }
  }

  private static QueryCompletionEvent toThriftQueryCompletionEvent(QueryCompletedEvent event)
  {
    QueryMetadata eventMetadata = event.getMetadata();
    QueryContext eventContext = event.getContext();
    QueryStatistics eventStat = event.getStatistics();

    QueryCompletionEvent thriftEvent =
      new com.twitter.presto.thriftjava.QueryCompletionEvent();

    thriftEvent.query_id = eventMetadata.getQueryId();
    thriftEvent.transaction_id = eventMetadata.getTransactionId().orElse(DASH);
    thriftEvent.user = eventContext.getUser();
    thriftEvent.principal = eventContext.getPrincipal().orElse(DASH);
    thriftEvent.source = eventContext.getSource().orElse(DASH);
    thriftEvent.server_version = eventContext.getServerVersion();
    thriftEvent.environment = eventContext.getEnvironment();
    thriftEvent.catalog = eventContext.getCatalog().orElse(DASH);
    thriftEvent.schema = eventContext.getSchema().orElse(DASH);
    Map<String, List<String>> queriedColumnsByTable = new HashMap<String, List<String>>();
    event.getIoMetadata().getInputs().forEach(input -> queriedColumnsByTable.put(String.format("%s.%s", input.getSchema(), input.getTable()), input.getColumns()));
    thriftEvent.queried_columns_by_table = queriedColumnsByTable;
    thriftEvent.remote_client_address = eventContext.getRemoteClientAddress().orElse(DASH);
    thriftEvent.user_agent = eventContext.getUserAgent().orElse(DASH);
    thriftEvent.query_state = QueryState.valueOf(eventMetadata.getQueryState());
    thriftEvent.uri = eventMetadata.getUri().toString();
    thriftEvent.query = eventMetadata.getQuery();
    thriftEvent.create_time_ms = event.getCreateTime().toEpochMilli();
    thriftEvent.execution_start_time_ms = event.getExecutionStartTime().toEpochMilli();
    thriftEvent.end_time_ms = event.getEndTime().toEpochMilli();
    thriftEvent.queued_time_ms = eventStat.getQueuedTime().toMillis();
    thriftEvent.query_wall_time_ms = eventStat.getWallTime().toMillis();
    thriftEvent.cumulative_memory_bytesecond = eventStat.getCumulativeMemory();
    thriftEvent.peak_memory_bytes = eventStat.getPeakMemoryBytes();
    if (eventStat.getAnalysisTime().isPresent()) {
      thriftEvent.analysis_time_ms = eventStat.getAnalysisTime().get().toMillis();
    }
    if (eventStat.getDistributedPlanningTime().isPresent()) {
      thriftEvent.distributed_planning_time_ms = eventStat.getDistributedPlanningTime().get().toMillis();
    }
    thriftEvent.total_bytes = eventStat.getTotalBytes();
    thriftEvent.query_stages = QueryStatsHelper.getQueryStages(eventMetadata);
    thriftEvent.operator_summaries = QueryStatsHelper.getOperatorSummaries(eventStat);
    thriftEvent.total_rows = eventStat.getTotalRows();
    thriftEvent.splits = eventStat.getCompletedSplits();
    if (event.getFailureInfo().isPresent()) {
      QueryFailureInfo eventFailureInfo = event.getFailureInfo().get();
      thriftEvent.error_code_id = eventFailureInfo.getErrorCode().getCode();
      thriftEvent.error_code_name = eventFailureInfo.getErrorCode().getName();
      thriftEvent.failure_type = eventFailureInfo.getFailureType().orElse(DASH);
      thriftEvent.failure_message = eventFailureInfo.getFailureMessage().orElse(DASH);
      thriftEvent.failure_task = eventFailureInfo.getFailureTask().orElse(DASH);
      thriftEvent.failure_host = eventFailureInfo.getFailureHost().orElse(DASH);
      thriftEvent.failures_json = eventFailureInfo.getFailuresJson();
    }

    return thriftEvent;
  }
}
