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
package com.facebook.presto.twitter.hive.thrift;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.TException;

import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS;

public class ThriftGeneralDeserializer
{
    private static final String REQUIRED_SERIALIZATION_CLASS = "com.facebook.presto.twitter.hive.thrift.ThriftGenericRow";

    public void initialize(Configuration conf, Properties properties)
    {
        String thriftClassName = properties.getProperty(SERIALIZATION_CLASS, null);
        checkCondition(thriftClassName != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive deserializer property: %s", SERIALIZATION_CLASS);
        checkCondition(thriftClassName.equals(REQUIRED_SERIALIZATION_CLASS), HIVE_INVALID_METADATA, SERIALIZATION_CLASS + thriftClassName + " cannot match " + REQUIRED_SERIALIZATION_CLASS);
        return;
    }

    public ThriftGenericRow deserialize(Writable writable, short[] thriftIds)
    {
        checkCondition(writable instanceof ThriftWritable, HIVE_UNKNOWN_ERROR, "Not an instance of ThriftWritable: " + writable);
        ThriftGenericRow row = (ThriftGenericRow) ((ThriftWritable) writable).get();
        try {
            row.parse(thriftIds);
        }
        catch (TException e) {
            throw new IllegalStateException("Generic row failed to parse values", e);
        }
        return row;
    }
}
