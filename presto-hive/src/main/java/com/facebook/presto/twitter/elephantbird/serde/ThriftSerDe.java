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
package com.twitter.elephantbird.hive.serde;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

/**
 * SerDe for working with {@link ThriftWritable} records.
 * This pairs well with {@link com.twitter.elephantbird.mapred.input.HiveMultiInputFormat}.
 */
public class ThriftSerDe implements SerDe
{
    @Override
    public void initialize(Configuration conf, Properties properties) throws SerDeException
    {
        String thriftClassName = properties.getProperty(Constants.SERIALIZATION_CLASS, null);
        if (thriftClassName == null) {
            throw new SerDeException("Required property " + Constants.SERIALIZATION_CLASS + " is null.");
        }

        Class thriftClass;
        try {
            thriftClass = conf.getClassByName(thriftClassName);
        }
        catch (ClassNotFoundException e) {
            throw new SerDeException("Failed getting class for " + thriftClassName);
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass()
    {
        return null;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException
    {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException
    {
        if (!(writable instanceof ThriftWritable)) {
            throw new SerDeException("Not an instance of ThriftWritable: " + writable);
        }
        return ((ThriftWritable) writable).get();
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException
    {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats()
    {
        return null;
    }
}
