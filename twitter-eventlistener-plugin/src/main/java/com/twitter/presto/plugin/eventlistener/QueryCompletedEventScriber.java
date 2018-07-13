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

        thriftEvent.setQuery_id(eventMetadata.getQueryId());
        thriftEvent.setTransaction_id(eventMetadata.getTransactionId().orElse(DASH));
        thriftEvent.setUser(eventContext.getUser());
        thriftEvent.setPrincipal(eventContext.getPrincipal().orElse(DASH));
        thriftEvent.setSource(eventContext.getSource().orElse(DASH));
        thriftEvent.setServer_version(eventContext.getServerVersion());
        thriftEvent.setEnvironment(eventContext.getEnvironment());
        thriftEvent.setCatalog(eventContext.getCatalog().orElse(DASH));
        thriftEvent.setSchema(eventContext.getSchema().orElse(DASH));
        Map<String, List<String>> queriedColumnsByTable = new HashMap<String, List<String>>();
        event.getIoMetadata().getInputs().forEach(input -> queriedColumnsByTable.put(String.format("%s.%s", input.getSchema(), input.getTable()), input.getColumns()));
        thriftEvent.setQueried_columns_by_table(queriedColumnsByTable);
        thriftEvent.setRemote_client_address(eventContext.getRemoteClientAddress().orElse(DASH));
        thriftEvent.setUser_agent(eventContext.getUserAgent().orElse(DASH));
        thriftEvent.setQuery_state(QueryState.valueOf(eventMetadata.getQueryState()));
        thriftEvent.setUri(eventMetadata.getUri().toString());
        thriftEvent.setQuery(eventMetadata.getQuery());
        thriftEvent.setCreate_time_ms(event.getCreateTime().toEpochMilli());
        thriftEvent.setExecution_start_time_ms(event.getExecutionStartTime().toEpochMilli());
        thriftEvent.setEnd_time_ms(event.getEndTime().toEpochMilli());
        thriftEvent.setQueued_time_ms(eventStat.getQueuedTime().toMillis());
        thriftEvent.setQuery_wall_time_ms(eventStat.getWallTime().toMillis());
        thriftEvent.setCumulative_memory_bytesecond(eventStat.getCumulativeMemory());
        thriftEvent.setPeak_memory_bytes(eventStat.getPeakTotalNonRevocableMemoryBytes());
        thriftEvent.setCpu_time_ms(eventStat.getCpuTime().toMillis());
        if (eventStat.getAnalysisTime().isPresent()) {
            thriftEvent.setAnalysis_time_ms(eventStat.getAnalysisTime().get().toMillis());
        }
        if (eventStat.getDistributedPlanningTime().isPresent()) {
            thriftEvent.setDistributed_planning_time_ms(eventStat.getDistributedPlanningTime().get().toMillis());
        }
        thriftEvent.setTotal_bytes(eventStat.getTotalBytes());
        thriftEvent.setQuery_stages(QueryStatsHelper.getQueryStages(eventMetadata));
        thriftEvent.setOperator_summaries(QueryStatsHelper.getOperatorSummaries(eventStat));
        thriftEvent.setTotal_rows(eventStat.getTotalRows());
        thriftEvent.setSplits(eventStat.getCompletedSplits());
        if (event.getFailureInfo().isPresent()) {
            QueryFailureInfo eventFailureInfo = event.getFailureInfo().get();
            thriftEvent.setError_code_id(eventFailureInfo.getErrorCode().getCode());
            thriftEvent.setError_code_name(eventFailureInfo.getErrorCode().getName());
            thriftEvent.setFailure_type(eventFailureInfo.getFailureType().orElse(DASH));
            thriftEvent.setFailure_message(eventFailureInfo.getFailureMessage().orElse(DASH));
            thriftEvent.setFailure_task(eventFailureInfo.getFailureTask().orElse(DASH));
            thriftEvent.setFailure_host(eventFailureInfo.getFailureHost().orElse(DASH));
            thriftEvent.setFailures_json(eventFailureInfo.getFailuresJson());
        }

        return thriftEvent;
    }
}
