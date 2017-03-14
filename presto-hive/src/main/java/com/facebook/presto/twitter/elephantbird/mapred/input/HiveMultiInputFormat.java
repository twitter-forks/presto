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
package com.facebook.presto.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.TypeRef;
import io.airlift.log.Logger;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Customized version of com.twitter.elephantbird.mapred.input.HiveMultiInputFormat
 */
@SuppressWarnings("deprecation")
public class HiveMultiInputFormat extends DeprecatedFileInputFormatWrapper<LongWritable, BinaryWritable>
{
    private static final Logger log = Logger.get(HiveMultiInputFormat.class);

    public HiveMultiInputFormat()
    {
        super(new MultiInputFormat());
    }

    private void initialize(FileSplit split, JobConf job) throws IOException
    {
        log.info("Initializing HiveMultiInputFormat for " + split + " with job " + job);

        String thriftClassName = null;

        thriftClassName = job.get(Constants.SERIALIZATION_CLASS);

        if (thriftClassName == null) {
            throw new RuntimeException(
                    "Required property " + Constants.SERIALIZATION_CLASS + " is null.");
        }

        try {
            Class thriftClass = job.getClassByName(thriftClassName);
            setInputFormatInstance(new MultiInputFormat(new TypeRef(thriftClass) {}));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed getting class for " + thriftClassName);
        }
    }

    @Override
    public RecordReader<LongWritable, BinaryWritable> getRecordReader(InputSplit split, JobConf job,
            Reporter reporter) throws IOException
    {
        initialize((FileSplit) split, job);
        return super.getRecordReader(split, job, reporter);
    }
}
