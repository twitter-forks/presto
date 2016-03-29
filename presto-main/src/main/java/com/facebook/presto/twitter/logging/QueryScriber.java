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
package com.facebook.presto.twitter.logging;

import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.event.query.QueryEventHandler;
import com.google.common.base.Optional;
import com.twitter.logging.BareFormatter$;
import com.twitter.logging.Level;
import com.twitter.logging.QueueingHandler;
import com.twitter.logging.ScribeHandler;
import com.twitter.presto.thriftjava.QueryState;
import io.airlift.log.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Base64;
import java.util.logging.LogRecord;

/**
 * Class that scribes query completion events
 */
public class QueryScriber implements QueryEventHandler<QueryCompletionEvent>
{
    private static final String SCRIBE_CATEGORY = "test_presto_query_complete";
    private static final int MAX_QUEUE_SIZE = 1000;

    private static final Logger log = Logger.get(QueryScriber.class);

    private QueueingHandler queueingHandler;

    // TSerializer is not thread safe
    private final ThreadLocal<TSerializer> serializer = new ThreadLocal<TSerializer>()
    {
        @Override protected TSerializer initialValue()
        {
          return new TSerializer();
        }
    };

    public QueryScriber()
    {
        ScribeHandler scribeHandler = new ScribeHandler(
                ScribeHandler.DefaultHostname(),
                ScribeHandler.DefaultPort(),
                SCRIBE_CATEGORY,
                ScribeHandler.DefaultBufferTime(),
                ScribeHandler.DefaultConnectBackoff(),
                ScribeHandler.DefaultMaxMessagesPerTransaction(),
                ScribeHandler.DefaultMaxMessagesToBuffer(),
                BareFormatter$.MODULE$,
                scala.Option.apply((Level) null));
        queueingHandler = new QueueingHandler(scribeHandler, MAX_QUEUE_SIZE);
    }

    @Override
    public void handle(QueryCompletionEvent event)
    {
        com.twitter.presto.thriftjava.QueryCompletionEvent thriftEvent = toThriftQueryCompletionEvent(event);
        Optional<String> message = serializeThriftToString(thriftEvent);

        if (message.isPresent()) {
            LogRecord logRecord = new LogRecord(Level.ALL, message.get());
            queueingHandler.publish(logRecord);
        }
        else {
            log.warn("Unable to serialize QueryCompletionEvent: " + event);
        }
    }

    /**
    * Serialize a thrift object to bytes, compress, then encode as a base64 string.
    */
    private Optional<String> serializeThriftToString(TBase thriftMessage)
    {
        try {
          return Optional.of(
                  Base64.getEncoder().encodeToString(serializer.get().serialize(thriftMessage)));
        }
        catch (TException e) {
            log.warn(e, "Could not serialize thrift object" + thriftMessage);
          return Optional.absent();
        }
    }

    private static com.twitter.presto.thriftjava.QueryCompletionEvent toThriftQueryCompletionEvent(QueryCompletionEvent event)
    {
        com.twitter.presto.thriftjava.QueryCompletionEvent thriftEvent =
                new com.twitter.presto.thriftjava.QueryCompletionEvent();

        thriftEvent.query_id = event.getQueryId();
        thriftEvent.transaction_id = event.getTransactionId();
        thriftEvent.user = event.getUser();
        thriftEvent.principal = event.getPrincipal();
        thriftEvent.source = event.getSource();
        thriftEvent.server_version = event.getServerVersion();
        thriftEvent.environment = event.getEnvironment();
        thriftEvent.catalog = event.getCatalog();
        thriftEvent.schema = event.getSchema();
        thriftEvent.remote_client_address = event.getRemoteClientAddress();
        thriftEvent.user_agent = event.getUserAgent();
        thriftEvent.query_state = QueryState.valueOf(event.getQueryState());
        thriftEvent.uri = event.getUri();
        thriftEvent.field_names = event.getFieldNames();
        thriftEvent.query = event.getQuery();
        thriftEvent.create_time_ms = event.getCreateTime().getMillis();
        thriftEvent.execution_start_time_ms = event.getExecutionStartTime().getMillis();
        thriftEvent.end_time_ms = event.getEndTime().getMillis();
        thriftEvent.queued_time_ms = event.getQueuedTimeMs();
        if (event.getAnalysisTimeMs() != null) {
            thriftEvent.analysis_time_ms = event.getAnalysisTimeMs();
        }
        if (event.getDistributedPlanningTimeMs() != null) {
            thriftEvent.distributed_planning_time_ms = event.getDistributedPlanningTimeMs();
        }
        if (event.getTotalSplitWallTimeMs() != null) {
            thriftEvent.total_split_wall_time_ms = event.getTotalSplitWallTimeMs();
        }
        if (event.getTotalSplitCpuTimeMs() != null) {
            thriftEvent.total_split_cpu_time_ms = event.getTotalSplitCpuTimeMs();
        }
        if (event.getTotalBytes() != null) {
            thriftEvent.total_bytes = event.getTotalBytes();
        }
        if (event.getTotalRows() != null) {
            thriftEvent.total_rows = event.getTotalRows();
        }
        thriftEvent.splits = event.getSplits();
        if (event.getErrorCode() != null) {
            thriftEvent.error_code_id = event.getErrorCode();
        }
        thriftEvent.error_code_name = event.getErrorCodeName();
        thriftEvent.failure_type = event.getFailureType();
        thriftEvent.failure_message = event.getFailureMessage();
        thriftEvent.failure_task = event.getFailureTask();
        thriftEvent.failure_host = event.getFailureHost();
        thriftEvent.output_stage_json = event.getOutputStageJson();
        thriftEvent.failures_json = event.getFailuresJson();
        thriftEvent.inputs_json = event.getInputsJson();
        thriftEvent.session_properties_json = event.getSessionPropertiesJson();
        thriftEvent.query_wall_time_ms = event.getQueryWallTimeMs();
        if (event.getBytesPerSec() != null) {
            thriftEvent.bytes_per_sec = event.getBytesPerSec();
        }
        if (event.getBytesPerCpuSec() != null) {
            thriftEvent.bytes_per_cpu_sec = event.getBytesPerCpuSec();
        }
        if (event.getRowsPerSec() != null) {
            thriftEvent.rows_per_sec = event.getRowsPerSec();
        }
        if (event.getRowsPerCpuSec() != null) {
            thriftEvent.rows_per_cpu_sec = event.getRowsPerCpuSec();
        }

        return thriftEvent;
    }
}
