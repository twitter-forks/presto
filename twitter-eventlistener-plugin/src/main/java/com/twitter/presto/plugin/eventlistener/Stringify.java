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
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.eventlistener.SplitFailureInfo;
import com.facebook.presto.spi.eventlistener.SplitStatistics;

import java.time.Duration;
import java.util.Optional;

class Stringify
{
  private static final String DASH = "-";
  private static final String FIELD_DELIMITER = " ,";
  private static final String OBJECT_DELIMITER = "\n\t";

  private static String getMillisOrElse(Optional<Duration> d, String defaultVal)
  {
    return d.isPresent() ? String.valueOf(d.get().toMillis()) : defaultVal;
  }

  private Stringify()
  {}

  public static String toString(QueryContext o)
  {
    String[] content = {"environment(%s)",
                        "user(%s)",
                        "userAgent(%s)",
                        "source(%s)",
                        "catalog(%s)",
                        "schema(%s)",
                        "principal(%s)",
                        "remoteClientAddress(%s)"};
    return String.format(
      String.format("QueryContext: %s", String.join(FIELD_DELIMITER, content)),
      o.getEnvironment(),
      o.getUser(),
      o.getUserAgent().orElse(DASH),
      o.getSource().orElse(DASH),
      o.getCatalog().orElse(DASH),
      o.getSchema().orElse(DASH),
      o.getPrincipal().orElse(DASH),
      o.getRemoteClientAddress().orElse(DASH));
  }

  public static String toString(QueryFailureInfo o)
  {
    String[] content = {"errorCode(%s)",
                        "failureType(%s)",
                        "failureMessage(%s)",
                        "failureTask(%s)",
                        "failureHost(%s)"};
    return String.format(
      String.format("QueryFailureInfo: %s", String.join(FIELD_DELIMITER, content)),
      o.getErrorCode().toString(),
      o.getFailureType().orElse(DASH),
      o.getFailureMessage().orElse(DASH),
      o.getFailureTask().orElse(DASH),
      o.getFailureHost().orElse(DASH));
  }

  public static String toString(QueryMetadata o)
  {
    String[] content = {"queryId(%s)",
                        "transactionId(%s)",
                        "query(%s)",
                        "queryState(%s)",
                        "uri(%s)",
                        "payload(%s)"};
    return String.format(
      String.format("QueryMetadata: %s", String.join(FIELD_DELIMITER, content)),
      o.getQueryId(),
      o.getTransactionId().orElse(DASH),
      o.getQuery(),
      o.getQueryState(),
      o.getUri().toString(),
      o.getPayload().orElse(DASH));
  }

  public static String toString(QueryStatistics o)
  {
    String[] content = {"cpuTime(%d ms)",
                        "wallTime(%d ms)",
                        "queuedTime(%d ms)",
                        "analysisTime(%s ms)",
                        "distributedPlanningTime(%s ms)",
                        "peakMemoryBytes(%d)",
                        "totalBytes(%d)",
                        "totalRows(%d)",
                        "completedSplits(%d)",
                        "complete(%b)"};
    return String.format(
      String.format("QueryStatistics: %s", String.join(FIELD_DELIMITER, content)),
      o.getCpuTime().toMillis(),
      o.getWallTime().toMillis(),
      o.getQueuedTime().toMillis(),
      getMillisOrElse(o.getAnalysisTime(), DASH),
      getMillisOrElse(o.getDistributedPlanningTime(), DASH),
      o.getPeakMemoryBytes(),
      o.getTotalBytes(),
      o.getTotalRows(),
      o.getCompletedSplits(),
      o.isComplete());
  }

  public static String toString(SplitFailureInfo o)
  {
    String[] content = {"failureType(%s)",
                        "failureMessage(%s)"};
    return String.format(
      String.format("SplitFailureInfo: %s", String.join(FIELD_DELIMITER, content)),
      o.getFailureType(),
      o.getFailureMessage());
  }

  public static String toString(SplitStatistics o)
  {
    String[] content = {"cpuTime(%d ms)",
                        "wallTime(%d ms)",
                        "queuedTime(%d ms)",
                        "userTime(%d ms)",
                        "completedReadTime(%d ms)",
                        "completedPositions(%d)",
                        "completedDataSizeBytes(%d)",
                        "timeToFirstByte(%s ms)",
                        "timeToLastByte(%s ms)"};
    return String.format(
      String.format("SplitStatistics: %s", String.join(FIELD_DELIMITER, content)),
      o.getCpuTime().toMillis(),
      o.getWallTime().toMillis(),
      o.getQueuedTime().toMillis(),
      o.getUserTime().toMillis(),
      o.getCompletedReadTime().toMillis(),
      o.getCompletedPositions(),
      o.getCompletedDataSizeBytes(),
      getMillisOrElse(o.getTimeToFirstByte(), DASH),
      getMillisOrElse(o.getTimeToLastByte(), DASH));
  }

  public static String toString(QueryCompletedEvent o)
  {
    String[] content = {toString(o.getMetadata()),
                        toString(o.getStatistics()),
                        toString(o.getContext()),
                        o.getFailureInfo().isPresent() ? toString(o.getFailureInfo().get()) : DASH,
                        String.format("creationTime: %s", o.getCreateTime().toString()),
                        String.format("executionStartTime: %s", o.getExecutionStartTime().toString()),
                        String.format("endTime: %s", o.getEndTime().toString())};
    return String.format("\nQueryCompletedEvent:\n\t%s\n", String.join(OBJECT_DELIMITER, content));
  }

  public static String toString(QueryCreatedEvent o)
  {
    String[] content = {String.format("createTime: %s", o.getCreateTime().toString()),
                        toString(o.getContext()),
                        toString(o.getMetadata())};
    return String.format("\nQueryCreatedEvent:\n\t%s\n", String.join(OBJECT_DELIMITER, content));
  }

  public static String toString(SplitCompletedEvent o)
  {
    return "";
  }
}
