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

import com.facebook.presto.spi.PrestoException;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static com.facebook.presto.hive.HiveUtil.getLzopIndexPath;
import static com.facebook.presto.hive.HiveUtil.isLzopCompressedFile;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS;

/**
 * Mirror of com.twitter.elephantbird.mapred.input.HiveMultiInputFormat allows to pass the thriftClassName
 * directly as a property of JobConfig and check lzo index existence when check splitability.
 * PR for twitter/elephant-bird:
 *      https://github.com/twitter/elephant-bird/pull/481
 *      https://github.com/twitter/elephant-bird/pull/485
 * Remove the class once #481 is included in a release
 */
@SuppressWarnings("deprecation")
public class ThriftGeneralInputFormat
        extends DeprecatedFileInputFormatWrapper<LongWritable, BinaryWritable>
{
    public ThriftGeneralInputFormat()
    {
        super(new MultiInputFormat());
    }

    private void initialize(FileSplit split, JobConf job) throws IOException
    {
        String thriftClassName = job.get(SERIALIZATION_CLASS);
        checkCondition(thriftClassName != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive deserializer property: %s", SERIALIZATION_CLASS);

        try {
            Class thriftClass = job.getClassByName(thriftClassName);
            setInputFormatInstance(new MultiInputFormat(new TypeRef(thriftClass) {}));
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(HIVE_INVALID_METADATA, format("Failed getting class for %s", thriftClassName));
        }
    }

    @Override
    public boolean isSplitable(FileSystem fs, Path filename)
    {
        if (isLzopCompressedFile(filename)) {
            Path indexFile = getLzopIndexPath(filename);
            try {
                return fs.exists(indexFile);
            }
            catch (IOException e) {
                return false;
            }
        }
        return super.isSplitable(fs, filename);
    }

    @Override
    public RecordReader<LongWritable, BinaryWritable> getRecordReader(
            InputSplit split,
            JobConf job,
            Reporter reporter)
            throws IOException
    {
        initialize((FileSplit) split, job);
        return super.getRecordReader(split, job, reporter);
    }
}
