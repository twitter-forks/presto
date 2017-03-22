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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveUtil.createRecordReader;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static java.util.Objects.requireNonNull;

public class ThriftHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private static final Set<String> THRIFT_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("com.facebook.presto.twitter.hive.thrift.ThriftGeneralSerDe")
            .add("com.facebook.presto.twitter.hive.thrift.ThriftGeneralDeserializer")
            .build();
    private final HdfsEnvironment hdfsEnvironment;
    private final ThriftFieldIdResolver thriftFieldIdResolver;

    @Inject
    public ThriftHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment, ThriftFieldIdResolver thriftFieldIdResolver)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.thriftFieldIdResolver = requireNonNull(thriftFieldIdResolver, "thriftFieldIdResolver is null");
    }

    @Override
    public Optional<RecordCursor> createRecordCursor(
            String clientId,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        if (!THRIFT_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema))) {
            return Optional.empty();
        }

        RecordReader<?, ?> recordReader;
        if (path.toString().endsWith(".index")) {
            recordReader = new DummyRecordReader<NullWritable, NullWritable>();
        }
        else {
            recordReader = hdfsEnvironment.doAs(session.getUser(),
                () -> createRecordReader(configuration, path, start, length, schema, columns));
        }

        return Optional.of(new ThriftHiveRecordCursor<>(
                genericRecordReader(recordReader),
                length,
                schema,
                columns,
                hiveStorageTimeZone,
                typeManager,
                thriftFieldIdResolver.initialize(schema)));
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> genericRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }

    private static final class DummyRecordReader<K, V> implements RecordReader<K, V>
    {
        public boolean next(K key, V value) throws IOException
        {
            return false;
        }

        public K createKey()
        {
            return null;
        }

        public V createValue()
        {
            return null;
        }

        public long getPos() throws IOException
        {
            return 0;
        }

        public float getProgress() throws IOException
        {
            return 0;
        }

        public void close() throws IOException {}
    }
}
