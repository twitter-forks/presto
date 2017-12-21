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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import com.hadoop.compression.lzo.LzoIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveStorageFormat.THRIFTBINARY;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static com.facebook.presto.hive.HiveUtil.createRecordReader;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.facebook.presto.hive.HiveUtil.getLzopIndexPath;
import static com.facebook.presto.hive.HiveUtil.isLzopCompressedFile;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS;

public class ThriftHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private static final String THRIFT_GENERIC_ROW = ThriftGenericRow.class.getName();
    private static final Set<String> THRIFT_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add(ThriftGeneralDeserializer.class.getName())
            .add(THRIFTBINARY.getSerDe())
            .build();
    private final HdfsEnvironment hdfsEnvironment;
    private final ThriftFieldIdResolverFactory thriftFieldIdResolverFactory;

    @Inject
    public ThriftHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment, ThriftFieldIdResolverFactory thriftFieldIdResolverFactory)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.thriftFieldIdResolverFactory = requireNonNull(thriftFieldIdResolverFactory, "thriftFieldIdResolverFactory is null");
    }

    @Override
    public Optional<RecordCursor> createRecordCursor(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        if (!THRIFT_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema))) {
            return Optional.empty();
        }

        // We only allow the table which specified its serialization class is compatible to
        // our thrift general row, if the SerDe is LazyBinarySerDe.
        if (THRIFTBINARY.getSerDe().equals(getDeserializerClassName(schema)) && !THRIFT_GENERIC_ROW.equals(getSerializationClassName(schema))) {
            return Optional.empty();
        }

        setPropertyIfUnset(schema, "elephantbird.mapred.input.bad.record.check.only.in.close", Boolean.toString(false));
        setPropertyIfUnset(schema, "elephantbird.mapred.input.bad.record.threshold", Float.toString(0.0f));

        // re-align split range
        if (isLzopCompressedFile(path)) {
            LzoIndex index = new LzoIndex();
            try {
                index = LzoIndex.readIndex(hdfsEnvironment.getFileSystem(session.getUser(), getLzopIndexPath(path), configuration), path);
            }
            catch (IOException ignored) {
                // ignored
            }
            if (index.isEmpty()) {
                if (start != 0) {
                    start = 0;
                    length = 0;
                }
                else {
                    length = fileSize;
                }
            }
            else {
                start = index.alignSliceStartToIndex(start, start + length);
                length = Math.min(index.alignSliceEndToIndex(start + length, fileSize), fileSize) - start;
            }
        }

        long finalStart = start;
        long finalLength = length;
        RecordReader<?, ?> recordReader = hdfsEnvironment.doAs(session.getUser(),
                () -> createRecordReader(configuration, path, finalStart, finalLength, schema, columns));

        return Optional.of(new ThriftHiveRecordCursor<>(
                genericRecordReader(recordReader),
                path,
                start,
                length,
                schema,
                columns,
                hiveStorageTimeZone,
                typeManager,
                thriftFieldIdResolverFactory.createResolver(schema)));
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> genericRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }

    private static void setPropertyIfUnset(Properties schema, String key, String value)
    {
        if (schema.getProperty(key) == null) {
            schema.setProperty(key, value);
        }
    }

    private static String getSerializationClassName(Properties schema)
    {
        String name = schema.getProperty(SERIALIZATION_CLASS);
        checkCondition(name != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive property: %s", SERIALIZATION_CLASS);
        return name;
    }
}
