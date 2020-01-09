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
package com.twitter.presto.gateway.query;

import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.twitter.presto.gateway.RequestInfo;

import java.util.Optional;

import static com.twitter.presto.gateway.query.QueryCategory.BATCH;
import static com.twitter.presto.gateway.query.QueryCategory.INTERACTIVE;
import static com.twitter.presto.gateway.query.QueryCategory.REALTIME;

public class QueryClassifier
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private QueryClassifier() {}

    public static QueryCategory classify(RequestInfo requestInfo)
    {
        String queryString = requestInfo.getQuery();
        Statement statement = SQL_PARSER.createStatement(queryString, new ParsingOptions());
        Optional<QueryType> type = StatementUtils.getQueryType(statement.getClass());

        if (type.isPresent()) {
            switch (type.get()) {
                case DATA_DEFINITION:
                case DESCRIBE:
                case EXPLAIN:
                    return REALTIME;
                case SELECT:
                case INSERT:
                case DELETE:
                case ANALYZE:
                    if (requestInfo.getSource().contains("schedule")) {
                        return BATCH;
                    }
                    return INTERACTIVE;
                default:
                    // by default query is distributed to BATCH
                    return BATCH;
            }
        }

        // by default query is distributed to BATCH
        return BATCH;
    }
}
