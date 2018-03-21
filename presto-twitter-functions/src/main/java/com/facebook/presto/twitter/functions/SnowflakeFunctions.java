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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/* size in
 * bits:  1                     41                      5     5        12
 *       +-+-----------------------------------------+-----+-----+------------+
 *       |0|              milliseconds               |clstr|instc|  sequence  |
 *       |0|              since twepoch              |  id |  id |   number   |
 *       +-+-----------------------------------------+-----+-----+------------+
 *        |
 *        +- Most significant bit
 */
public class SnowflakeFunctions
{
    private static final long SequenceNumBits = 12L;
    private static final long MaxSequenceNum = (1L << SequenceNumBits) - 1;
    private static final long SequenceNumMask = MaxSequenceNum;

    private static final long InstanceIdBits = 5L;
    private static final long MaxInstanceId = (1L << InstanceIdBits) - 1;
    private static final long InstanceIdShift = SequenceNumBits;
    private static final long InstanceIdMask = MaxInstanceId << InstanceIdShift;

    private static final long ClusterIdBits = 5L;
    private static final long MaxClusterId = (1L << ClusterIdBits) - 1;
    private static final long ClusterIdShift = InstanceIdShift + InstanceIdBits;
    private static final long ClusterIdMask = MaxClusterId << ClusterIdShift;

    private static final long TimestampBits = 41L;
    private static final long MaxTimestamp = (1L << TimestampBits) - 1;
    private static final long TimestampShift = ClusterIdShift + ClusterIdBits;
    private static final long TimestampMask = MaxTimestamp << TimestampShift;

    /* Twepoch is 2010-11-04T01:42:54Z.
     * Value is in millis since Unix Epoch 1970-01-01T00:00:00Z.
     */
    private static final long Twepoch = 1288834974657L;
    private static final long FirstSnowflakeIdUnixTime = Twepoch + TimeUnit.DAYS.toMillis(1); // 1 day after Twepoch.
    private static final long FirstSnowflakeId = firstSnowflakeIdFor(FirstSnowflakeIdUnixTime);

    private SnowflakeFunctions()
    {
    }

    @ScalarFunction("is_snowflake")
    @Description("Check if a BIGINT is a Snowflake ID")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isSnowflakeId(@SqlType(StandardTypes.BIGINT) long id)
    {
        return id >= FirstSnowflakeId;
    }

    @ScalarFunction("first_snowflake_for")
    @Description("Return the first snowflake ID given a timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long firstSnowflakeIdFor(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (timestamp < FirstSnowflakeIdUnixTime) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid UnixTimeMillis: UnixTimeMillis[" + timestamp + "] >= FirstSnowflakeIdUnixTime");
        }
        return ((timestamp - Twepoch) << TimestampShift);
    }

    @ScalarFunction("timestamp_from_snowflake")
    @Description("Return the timestamp given a snowflake ID")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampFromSnowflakeId(@SqlType(StandardTypes.BIGINT) long id)
    {
        if (!isSnowflakeId(id)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Not a Snowflake Id: " + id);
        }
        return ((id & TimestampMask) >> TimestampShift) + Twepoch;
    }

    @ScalarFunction("cluster_id_from_snowflake")
    @Description("Return the cluster id given a snowflake ID")
    @SqlType(StandardTypes.BIGINT)
    public static long clusterIdFromSnowflakeId(@SqlType(StandardTypes.BIGINT) long id)
    {
        if (!isSnowflakeId(id)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Not a Snowflake Id: " + id);
        }
        return (id & ClusterIdMask) >> ClusterIdShift;
    }

    @ScalarFunction("instance_id_from_snowflake")
    @Description("Return the instance id given a snowflake ID")
    @SqlType(StandardTypes.BIGINT)
    public static long instanceIdFromSnowflakeId(@SqlType(StandardTypes.BIGINT) long id)
    {
        if (!isSnowflakeId(id)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Not a Snowflake Id: " + id);
        }
        return (id & InstanceIdMask) >> InstanceIdShift;
    }

    @ScalarFunction("sequence_num_from_snowflake")
    @Description("Return the sequence number given a snowflake ID")
    @SqlType(StandardTypes.BIGINT)
    public static long sequenceNumFromSnowflakeId(@SqlType(StandardTypes.BIGINT) long id)
    {
        if (!isSnowflakeId(id)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Not a Snowflake Id: " + id);
        }
        return id & SequenceNumMask;
    }
}
