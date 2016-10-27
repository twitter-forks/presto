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
import java.util.Optional;
import java.util.logging.LogRecord;

/**
 * Class that scribes query completion events
 */
public class QueryScriber
{
  private static final String QUERY_COMPLETED_SCRIBE_CATEGORY = "presto_query_completion";
  private static final String DASH = "-";
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
      QUERY_COMPLETED_SCRIBE_CATEGORY,
      ScribeHandler.DefaultBufferTime(),
      ScribeHandler.DefaultConnectBackoff(),
      ScribeHandler.DefaultMaxMessagesPerTransaction(),
      ScribeHandler.DefaultMaxMessagesToBuffer(),
      BareFormatter$.MODULE$,
      scala.Option.apply((Level) null));
    queueingHandler = new QueueingHandler(scribeHandler, MAX_QUEUE_SIZE);
  }

  public void handle(QueryCompletedEvent event)
  {
    com.twitter.presto.thriftjava.QueryCompletionEvent thriftEvent = toThriftQueryCompletionEvent(event);
    Optional<String> message = serializeThriftToString(thriftEvent);

    if (message.isPresent()) {
      LogRecord logRecord = new LogRecord(Level.ALL, message.get());
      queueingHandler.publish(logRecord);
    }
    else {
      log.warn("Unable to serialize QueryCompletedEvent: " + event);
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
      return Optional.empty();
    }
  }

  private static com.twitter.presto.thriftjava.QueryCompletionEvent toThriftQueryCompletionEvent(QueryCompletedEvent event)
  {
    QueryMetadata eventMetadata = event.getMetadata();
    QueryContext eventContext = event.getContext();
    QueryStatistics eventStat = event.getStatistics();

    com.twitter.presto.thriftjava.QueryCompletionEvent thriftEvent =
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
    if (eventStat.getAnalysisTime().isPresent()) {
      thriftEvent.analysis_time_ms = eventStat.getAnalysisTime().get().toMillis();
    }
    if (eventStat.getDistributedPlanningTime().isPresent()) {
      thriftEvent.distributed_planning_time_ms = eventStat.getDistributedPlanningTime().get().toMillis();
    }
    thriftEvent.total_bytes = eventStat.getTotalBytes();
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
