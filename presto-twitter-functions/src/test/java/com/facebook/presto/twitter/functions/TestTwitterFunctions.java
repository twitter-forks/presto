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
package com.facebook.presto.twitter.functions;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TestTwitterFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        functionAssertions.addFunctions(extractFunctions(new TwitterFunctionsPlugin().getFunctions()));
    }

    @Test
    public void testStr2Array()
    {
        assertFunction("SPLIT_EVERY('')", new ArrayType(createVarcharType(0)), ImmutableList.of(""));
        assertFunction("SPLIT_EVERY('abc')", new ArrayType(createVarcharType(3)), ImmutableList.of("a", "b", "c"));
        assertFunction("SPLIT_EVERY('a.b.c')", new ArrayType(createVarcharType(5)), ImmutableList.of("a", ".", "b", ".", "c"));
        assertFunction("SPLIT_EVERY('...')", new ArrayType(createVarcharType(3)), ImmutableList.of(".", ".", "."));
        // Test str_to_array for non-ASCII
        assertFunction("SPLIT_EVERY('\u4FE1\u5FF5,\u7231,\u5E0C\u671B')", new ArrayType(createVarcharType(7)), ImmutableList.of("\u4FE1", "\u5FF5", ",", "\u7231", ",", "\u5E0C", "\u671B"));
        // Test argument length
        assertFunction("SPLIT_EVERY('a.b.c', 2)", new ArrayType(createVarcharType(5)), ImmutableList.of("a.", "b.", "c"));
        // Test argument limit
        assertFunction("SPLIT_EVERY('a.b.c', 2, 1)", new ArrayType(createVarcharType(5)), ImmutableList.of("a.b.c"));
        assertFunction("SPLIT_EVERY('a.b.c', 2, 2)", new ArrayType(createVarcharType(5)), ImmutableList.of("a.", "b.c"));
    }

    private static SqlTimestamp toTimestampUTC(long millis)
    {
        return new SqlTimestamp(millis, UTC_KEY);
    }

    @Test
    public void testSnowflake()
    {
        assertFunction("IS_SNOWFLAKE(1000)", BOOLEAN, false);
        assertFunction("IS_SNOWFLAKE(265605588183052288)", BOOLEAN, true);
        assertFunction("IS_SNOWFLAKE(-265605588183052288)", BOOLEAN, false);

        assertFunction("FIRST_SNOWFLAKE_FOR(from_unixtime(1352160281.593))", BIGINT, 265605588182892544L);
        assertInvalidFunction("FIRST_SNOWFLAKE_FOR(from_unixtime(1000))", "Invalid UnixTimeMillis: UnixTimeMillis[1000000] >= FirstSnowflakeIdUnixTime");

        assertFunction("TIMESTAMP_FROM_SNOWFLAKE(265605588183052288)", TIMESTAMP, toTimestampUTC(1352160281593L));
        assertInvalidFunction("TIMESTAMP_FROM_SNOWFLAKE(1000)", "Not a Snowflake Id: 1000");

        assertFunction("CLUSTER_ID_FROM_SNOWFLAKE(265605588183052288)", BIGINT, 1L);
        assertInvalidFunction("CLUSTER_ID_FROM_SNOWFLAKE(1000)", "Not a Snowflake Id: 1000");

        assertFunction("INSTANCE_ID_FROM_SNOWFLAKE(265605588183052288)", BIGINT, 7L);
        assertInvalidFunction("INSTANCE_ID_FROM_SNOWFLAKE(1000)", "Not a Snowflake Id: 1000");

        assertFunction("SEQUENCE_NUM_FROM_SNOWFLAKE(265605588183052288)", BIGINT, 0L);
        assertInvalidFunction("SEQUENCE_NUM_FROM_SNOWFLAKE(1000)", "Not a Snowflake Id: 1000");
    }
}
