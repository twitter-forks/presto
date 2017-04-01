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

import com.hadoop.compression.lzo.LzoIndex;
import com.twitter.elephantbird.mapred.input.DeprecatedFileInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.TypeRef;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static java.lang.Math.toIntExact;
import static org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS;

/**
 * Customized version of com.twitter.elephantbird.mapred.input.HiveMultiInputFormat
 */
@SuppressWarnings("deprecation")
public class ThriftGeneralInputFormat extends DeprecatedFileInputFormatWrapper<LongWritable, BinaryWritable>
{
    public static final PathFilter lzoSuffixFilter = new PathFilter() {
        @Override
        public boolean accept(Path path)
        {
            return path.toString().endsWith(".lzo");
        }
    };

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
            throw new RuntimeException("Failed getting class for " + thriftClassName);
        }
    }

    @Override
    public RecordReader<LongWritable, BinaryWritable> getRecordReader(
            InputSplit split,
            JobConf job,
            Reporter reporter)
            throws IOException
    {
        job.setBoolean("elephantbird.mapred.input.bad.record.check.only.in.close", false);
        job.setFloat("elephantbird.mapred.input.bad.record.threshold", 0.0f);

        initialize((FileSplit) split, job);
        return super.getRecordReader(split, job, reporter);
    }

    public static Path getLzoIndexPath(Path lzoPath)
    {
        return lzoPath.suffix(LzoIndex.LZO_INDEX_SUFFIX);
    }

    public static InputSplit[] getLzoSplits(
            JobConf job,
            LocatedFileStatus file)
            throws IOException
    {
        InputSplit[] splits = new InputSplit[1];
        splits[0] = new FileSplit(file.getPath(), 0, file.getLen(), job);
        return splits;
    }

    public static InputSplit[] getLzoSplits(
            JobConf job,
            LocatedFileStatus file,
            FileSystem indexFilesystem,
            AtomicInteger remainingInitialSplits,
            DataSize maxInitialSplitSize,
            DataSize maxSplitSize)
            throws IOException
    {
        LzoIndex index = LzoIndex.readIndex(indexFilesystem, file.getPath());
        if (index.isEmpty()) {
            return getLzoSplits(job, file);
        }

        List<InputSplit> splits = new ArrayList<>();
        long chunkOffset = 0;
        while (chunkOffset < file.getLen()) {
            long targetChunkSize;
            if (remainingInitialSplits.decrementAndGet() >= 0) {
                targetChunkSize = maxInitialSplitSize.toBytes();
            }
            else {
                long maxBytes = maxSplitSize.toBytes();
                int chunks = toIntExact((long) Math.ceil((file.getLen() - chunkOffset) * 1.0 / maxBytes));
                targetChunkSize = (long) Math.ceil((file.getLen() - chunkOffset) * 1.0 / chunks);
            }
            long chunkEnd = index.alignSliceEndToIndex(chunkOffset + targetChunkSize, file.getLen());

            splits.add(new FileSplit(file.getPath(), chunkOffset, chunkEnd - chunkOffset, job));
            chunkOffset = chunkEnd;
        }

        return splits.toArray(new InputSplit[0]);
    }
}
