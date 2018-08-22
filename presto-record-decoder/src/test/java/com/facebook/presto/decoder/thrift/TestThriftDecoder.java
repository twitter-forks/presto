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
package com.facebook.presto.decoder.thrift;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.decoder.thrift.tweep.Location;
import com.facebook.presto.decoder.thrift.tweep.Tweet;
import com.facebook.presto.decoder.thrift.tweep.TweetType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestThriftDecoder
{
    private static final ThriftRowDecoderFactory DECODER_FACTORY = new ThriftRowDecoderFactory();

    @Test
    public void testSimple()
            throws Exception
    {
        Tweet tweet = new Tweet(1, "newUser", "hello world")
                .setLoc(new Location(1234, 5678))
                .setAge((short) 26)
                .setB((byte) 10)
                .setIsDeleted(false)
                .setTweetType(TweetType.REPLY)
                .setFullId(1234567)
                .setPic("abc".getBytes())
                .setAttr(ImmutableMap.of("a", "a"));

        // schema
        DecoderTestColumnHandle col1 = new DecoderTestColumnHandle(1, "user_id", IntegerType.INTEGER, "1", "thrift", null, false, false, false);
        DecoderTestColumnHandle col2 = new DecoderTestColumnHandle(2, "username", createVarcharType(100), "2", "thrift", null, false, false, false);
        DecoderTestColumnHandle col3 = new DecoderTestColumnHandle(3, "text", createVarcharType(100), "3", "thrift", null, false, false, false);
        DecoderTestColumnHandle col4 = new DecoderTestColumnHandle(4, "loc.latitude", DoubleType.DOUBLE, "4/1", "thrift", null, false, false, false);
        DecoderTestColumnHandle col5 = new DecoderTestColumnHandle(5, "loc.longitude", DoubleType.DOUBLE, "4/2", "thrift", null, false, false, false);
        DecoderTestColumnHandle col6 = new DecoderTestColumnHandle(6, "tweet_type", BigintType.BIGINT, "5", "thrift", null, false, false, false);
        DecoderTestColumnHandle col7 = new DecoderTestColumnHandle(7, "is_deleted", BooleanType.BOOLEAN, "6", "thrift", null, false, false, false);
        DecoderTestColumnHandle col8 = new DecoderTestColumnHandle(8, "b", TinyintType.TINYINT, "7", "thrift", null, false, false, false);
        DecoderTestColumnHandle col9 = new DecoderTestColumnHandle(9, "age", SmallintType.SMALLINT, "8", "thrift", null, false, false, false);
        DecoderTestColumnHandle col10 = new DecoderTestColumnHandle(10, "full_id", BigintType.BIGINT, "9", "thrift", null, false, false, false);
        DecoderTestColumnHandle col11 = new DecoderTestColumnHandle(11, "pic", VarbinaryType.VARBINARY, "10", "thrift", null, false, false, false);
        DecoderTestColumnHandle col12 = new DecoderTestColumnHandle(12, "language", createVarcharType(100), "16", "thrift", null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        TMemoryBuffer transport = new TMemoryBuffer(4096);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        tweet.write(protocol);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(transport.getArray(), null)
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, col1, 1);
        checkValue(decodedRow, col2, "newUser");
        checkValue(decodedRow, col3, "hello world");
        checkValue(decodedRow, col4, 1234);
        checkValue(decodedRow, col5, 5678);
        checkValue(decodedRow, col6, TweetType.REPLY.getValue());
        checkValue(decodedRow, col7, false);
        checkValue(decodedRow, col8, 10);
        checkValue(decodedRow, col9, 26);
        checkValue(decodedRow, col10, 1234567);
        checkValue(decodedRow, col11, "abc");
        checkValue(decodedRow, col12, "english");
    }
}
