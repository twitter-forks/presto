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
package com.facebook.presto.kafka;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.decoder.FieldValueProviders.booleanValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.bytesValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.longValueProvider;
import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific record set. Returns a cursor for a topic which iterates over a Kafka partition segment.
 */
public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KafkaRecordSet.class);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final KafkaSplit split;
    private final KafkaSimpleConsumerManager consumerManager;
    private final int fetchSize;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    KafkaRecordSet(KafkaSplit split,
            KafkaSimpleConsumerManager consumerManager,
            List<KafkaColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder,
            int fetchSize)
    {
        this.split = requireNonNull(split, "split is null");

        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
        this.fetchSize = fetchSize;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KafkaRecordCursor(split.getStartTs(), split.getEndTs());
    }

    public class KafkaRecordCursor
            implements RecordCursor
    {
        private long totalBytes;
        private long totalMessages;
        private long cursorOffset = split.getStart();
        private Iterator<MessageAndOffset> messageAndOffsetIterator;
        private long fetchedSize;
        private final AtomicBoolean reported = new AtomicBoolean();

        private final long startTs;
        private final long endTs;

        private FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];

        KafkaRecordCursor(long startTs, long endTs)
        {
            this.startTs = startTs;
            this.endTs = endTs;
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (true) {
                if (cursorOffset >= split.getEnd()) {
                    return endOfData(1); // Split end is exclusive.
                }

                try {
                    // Create a fetch request
                    openFetchRequest();
                    if (cursorOffset >= split.getEnd()) {
                        return endOfData(2); // Split end is exclusive.
                    }
                    if (messageAndOffsetIterator.hasNext()) {
                        MessageAndOffset currentMessageAndOffset = messageAndOffsetIterator.next();
                        return nextRow(currentMessageAndOffset);
                    }
                }
                catch (OffsetOutOfRangeException e) {
                    e.printStackTrace();
                    return endOfData(4);
                }
                messageAndOffsetIterator = null;
            }
        }

        private boolean endOfData(int from)
        {
            if (!reported.getAndSet(true)) {
                log.info("Found (from %d) a total of %d messages with %d bytes (%d compressed bytes expected). Last Offset: %d (%d, %d)",
                        from, totalMessages, totalBytes, split.getEnd() - split.getStart(),
                        cursorOffset, split.getStart(), split.getEnd());
            }
            return false;
        }

        private boolean nextRow(MessageAndOffset messageAndOffset)
        {
            totalBytes += messageAndOffset.message().payloadSize();
            totalMessages++;

            byte[] keyData = EMPTY_BYTE_ARRAY;
            byte[] messageData = EMPTY_BYTE_ARRAY;

            ByteBuffer message = messageAndOffset.message().payload();
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }

            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData, null);
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = messageDecoder.decodeRow(messageData, null);

            for (DecoderColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isInternal()) {
                    KafkaInternalFieldDescription fieldDescription = KafkaInternalFieldDescription.forColumnName(columnHandle.getName());
                    switch (fieldDescription) {
                        case SEGMENT_COUNT_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(totalMessages));
                            break;
                        case PARTITION_OFFSET_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(messageAndOffset.offset()));
                            break;
                        case MESSAGE_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(messageData));
                            break;
                        case MESSAGE_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(messageData.length));
                            break;
                        case KEY_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                            break;
                        case KEY_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                            break;
                        case KEY_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedKey.isPresent()));
                            break;
                        case MESSAGE_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedValue.isPresent()));
                            break;
                        case PARTITION_ID_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(split.getPartitionId()));
                            break;
                        case SEGMENT_START_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(startTs));
                            break;
                        case SEGMENT_END_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(endTs));
                            break;
                        case OFFSET_TIMESTAMP_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(populateOffsetTimestamp(startTs, endTs)));
                        default:
                            throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                    }
                }
            }

            decodedKey.ifPresent(currentRowValuesMap::putAll);
            decodedValue.ifPresent(currentRowValuesMap::putAll);

            for (int i = 0; i < columnHandles.size(); i++) {
                ColumnHandle columnHandle = columnHandles.get(i);
                currentRowValues[i] = currentRowValuesMap.get(columnHandle);
            }

            return true; // Advanced successfully.
        }

        private void openFetchRequest()
        {
            if (messageAndOffsetIterator == null) {
                log.info("Fetching %d bytes from partition %d @offset %d (%d - %d) -- %d messages read so far",
                        fetchSize, split.getPartitionId(), cursorOffset, split.getStart(), split.getEnd(), totalMessages);
                cursorOffset += fetchedSize;
                if (cursorOffset < split.getEnd()) {
                    FetchRequest req = new FetchRequest(split.getTopicName(), split.getPartitionId(), cursorOffset, fetchSize);
                    SimpleConsumer consumer = consumerManager.getConsumer(split.getLeader());

                    ByteBufferMessageSet fetch = consumer.fetch(req);
                    log.debug("\t...fetched %s bytes, validBytes=%s, initialOffset=%s", fetch.sizeInBytes(), fetch.validBytes(), fetch.getInitialOffset());
                    int errorCode = fetch.getErrorCode();
                    if (errorCode != ErrorMapping.NoError() && errorCode != ErrorMapping.OffsetOutOfRangeCode()) {
                        log.warn("Fetch response has error: %d", errorCode);
                        throw new PrestoException(KAFKA_SPLIT_ERROR, "could not fetch data from Kafka, error code is '" + errorCode + "'");
                    }

                    fetchedSize = fetch.validBytes();
                    messageAndOffsetIterator = fetch.iterator();
                }
            }
        }

        private long populateOffsetTimestamp(long startTs, long endTs)
        {
            if (startTs == OffsetRequest.EarliestTime()) {
                startTs = 0;
            }

            if (endTs == OffsetRequest.LatestTime()) {
                endTs = System.currentTimeMillis();
            }

            return startTs + (endTs - startTs) / 2;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return getFieldValueProvider(field, boolean.class).getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            return getFieldValueProvider(field, long.class).getLong();
        }

        @Override
        public double getDouble(int field)
        {
            return getFieldValueProvider(field, double.class).getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            return getFieldValueProvider(field, Slice.class).getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            return getFieldValueProvider(field, Block.class).getBlock();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return currentRowValues[field] == null || currentRowValues[field].isNull();
        }

        private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            checkFieldType(field, expectedType);
            return currentRowValues[field];
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
        }
    }
}
