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
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Class that logs query events to a file
 */
public class QueryLogger implements QueryEventHandler<QueryCompletionEvent>
{
    private static final int MAX_QUERY_LENGTH = 1000;
    private static final String DASH = "-";
    private static final String COLON = ":";
    private static final String SPACE = " ";
    private static final String ELIPSIS = "...";
    private static final String QUERY_COMPLETION = "QueryCompletion";

    private static final Logger log = Logger.get(QueryLogger.class);

    @Override
    public void handle(QueryCompletionEvent event)
    {
        String errorCode = DASH;
        if (event.getErrorCode() != null) {
            if (event.getErrorCodeName() != null) {
                errorCode = event.getErrorCodeName() + COLON + event.getErrorCode();
            }
        }

        Duration duration = (new Duration(
                event.getQueryWallTimeMs(), TimeUnit.MILLISECONDS))
                .convertToMostSuccinctTimeUnit();

        log.info(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
                QUERY_COMPLETION, event.getQueryId(), toLogValue(event.getRemoteClientAddress()),
                event.getQueryState(), errorCode, event.getUser(), duration,
                event.getSplits(), event.getTotalRows(), event.getTotalBytes(),
                cleanseAndTrimQuery(event.getQuery())));
    }

    private static String toLogValue(Object object)
    {
        if (object == null) {
            return DASH;
        }
        else {
            return object.toString();
        }
    }

    private static String cleanseAndTrimQuery(String query)
    {
        if (query.length() > MAX_QUERY_LENGTH) {
            query = query.substring(0, MAX_QUERY_LENGTH) + ELIPSIS;
        }
        return query.replace(System.getProperty("line.separator"), SPACE);
    }
}
