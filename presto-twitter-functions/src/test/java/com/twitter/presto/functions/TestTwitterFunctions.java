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
package com.twitter.presto.functions;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;

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
        return new SqlTimestamp(millis, TestingSession.DEFAULT_TIME_ZONE_KEY);
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

    @Test
    public void testKeyOfMaxValue()
    {
        assertFunction("KEY_OF_MAX_VALUE(MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]))", createVarcharType(3), "bar");
        assertFunction("KEY_OF_MAX_VALUE(CAST(MAP(ARRAY[100.0, 200.0], ARRAY[1, 2]) AS MAP(DOUBLE, BIGINT)))", DOUBLE, 200.0);
        assertFunction("KEY_OF_MAX_VALUE(CAST(MAP(ARRAY[100, 200], ARRAY[1, 2]) AS MAP(BIGINT, BIGINT)))", BIGINT, 200L);
        assertFunction("KEY_OF_MAX_VALUE(CAST(MAP(ARRAY[1, 0], ARRAY[1,2]) AS MAP(BOOLEAN, BIGINT)))", BOOLEAN, false);

        assertFunction("KEY_OF_MAX_VALUE(CAST(MAP(ARRAY[1, 0], ARRAY[2,2]) AS MAP(BOOLEAN, BIGINT)))", BOOLEAN, null);
    }
}
