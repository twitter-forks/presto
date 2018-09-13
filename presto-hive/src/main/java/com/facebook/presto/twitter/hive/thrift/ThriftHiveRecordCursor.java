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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.closeWithSuppression;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.facebook.presto.hive.HiveUtil.isRowType;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ThriftHiveRecordCursor<K, V extends Writable>
        implements RecordCursor
{
    private static final Logger log = Logger.get(ThriftHiveRecordCursor.class);
    private static final short NON_EXISTED_THRIFT_ID = (short) -1;
    private final RecordReader<K, V> recordReader;
    private final K key;
    private final V value;

    private final ThriftGeneralDeserializer deserializer;

    private final Type[] types;
    private final HiveType[] hiveTypes;
    private final int[] hiveIndexs;
    private final short[] thriftIds;
    private final HiveThriftFieldIdGroup thriftFieldIdGroup;

    private final boolean[] loaded;
    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final Object[] objects;
    private final boolean[] nulls;

    private final Path path;
    private final long start;
    private final long totalBytes;
    private final DateTimeZone hiveStorageTimeZone;

    private final ThriftFieldIdResolver thriftFieldIdResolver;

    private long completedBytes;
    private ThriftGenericRow rowData;
    private boolean closed;

    public ThriftHiveRecordCursor(
            RecordReader<K, V> recordReader,
            Path path,
            long start,
            long totalBytes,
            Properties splitSchema,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ThriftFieldIdResolver thriftFieldIdResolver)
    {
        requireNonNull(recordReader, "recordReader is null");
        requireNonNull(path, "path is null");
        checkArgument(start >= 0, "start is negative");
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(thriftFieldIdResolver, "thriftFieldIdResolver is null");

        this.recordReader = recordReader;
        this.path = path;
        this.start = start;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();
        this.hiveStorageTimeZone = hiveStorageTimeZone;
        this.thriftFieldIdResolver = thriftFieldIdResolver;

        this.deserializer = new ThriftGeneralDeserializer(new Configuration(false), splitSchema);

        int size = columns.size();

        this.types = new Type[size];
        this.hiveTypes = new HiveType[size];
        this.hiveIndexs = new int[size];
        this.thriftIds = new short[size];

        this.loaded = new boolean[size];
        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.objects = new Object[size];
        this.nulls = new boolean[size];

        ImmutableMap.Builder<Short, HiveThriftFieldIdGroup> thriftFieldIdGroupBuilder = ImmutableMap.builder();
        // initialize data columns
        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);
            checkState(column.getColumnType() == REGULAR, "column type must be regular");

            types[i] = typeManager.getType(column.getTypeSignature());
            hiveTypes[i] = column.getHiveType();
            hiveIndexs[i] = column.getHiveColumnIndex();
            thriftIds[i] = getThriftIdWithFailOver(thriftFieldIdResolver, hiveIndexs[i]);
            if (column.getFieldSet().isPresent()) {
                thriftFieldIdGroupBuilder.put(thriftIds[i], HiveThriftFieldIdGroup.create(types[i], column.getFieldSet().get(), thriftFieldIdResolver));
            }
            else {
                thriftFieldIdGroupBuilder.put(thriftIds[i], new HiveThriftFieldIdGroup(ImmutableMap.of()));
            }
        }

        this.thriftFieldIdGroup = new HiveThriftFieldIdGroup(thriftFieldIdGroupBuilder.build());

        // close immediately if the number of totalBytes is zero
        if (totalBytes == 0) {
            close();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        if (!closed) {
            updateCompletedBytes();
        }
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    private void updateCompletedBytes()
    {
        try {
            long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
        }
        catch (IOException ignored) {
        }
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (closed || !recordReader.next(key, value)) {
                close();
                return false;
            }

            // reset loaded flags
            Arrays.fill(loaded, false);

            // decode value
            rowData = deserializer.deserialize(value, thriftFieldIdGroup);

            return true;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(this, e);
            throw new PrestoException(HIVE_CURSOR_ERROR,
                format("Failed to read split: %s %s:%s, total bytes: %s, completed bytes: %s",
                    path, start, start + totalBytes, totalBytes, completedBytes),
                e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, boolean.class);
        if (!loaded[fieldId]) {
            parseBooleanColumn(fieldId);
        }
        return booleans[fieldId];
    }

    private void parseBooleanColumn(int column)
    {
        loaded[column] = true;

        Object fieldValue = rowData.getFieldValueForThriftId(thriftIds[column]);

        if (fieldValue == null) {
            nulls[column] = true;
        }
        else {
            booleans[column] = (Boolean) fieldValue;
            nulls[column] = false;
        }
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, long.class);
        if (!loaded[fieldId]) {
            parseLongColumn(fieldId);
        }
        return longs[fieldId];
    }

    private void parseLongColumn(int column)
    {
        loaded[column] = true;

        Object fieldValue = rowData.getFieldValueForThriftId(thriftIds[column]);

        if (fieldValue == null) {
            nulls[column] = true;
        }
        else {
            longs[column] = getLongExpressedValue(fieldValue, hiveStorageTimeZone);
            nulls[column] = false;
        }
    }

    private static long getLongExpressedValue(Object value, DateTimeZone hiveTimeZone)
    {
        if (value instanceof Date) {
            long storageTime = ((Date) value).getTime();
            // convert date from VM current time zone to UTC
            long utcMillis = storageTime + DateTimeZone.getDefault().getOffset(storageTime);
            return TimeUnit.MILLISECONDS.toDays(utcMillis);
        }
        if (value instanceof Timestamp) {
            // The Hive SerDe parses timestamps using the default time zone of
            // this JVM, but the data might have been written using a different
            // time zone. We need to convert it to the configured time zone.

            // the timestamp that Hive parsed using the JVM time zone
            long parsedJvmMillis = ((Timestamp) value).getTime();

            // remove the JVM time zone correction from the timestamp
            DateTimeZone jvmTimeZone = DateTimeZone.getDefault();
            long hiveMillis = jvmTimeZone.convertUTCToLocal(parsedJvmMillis);

            // convert to UTC using the real time zone for the underlying data
            long utcMillis = hiveTimeZone.convertLocalToUTC(hiveMillis, false);

            return utcMillis;
        }
        if (value instanceof Float) {
            return floatToRawIntBits(((Float) value));
        }
        return ((Number) value).longValue();
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
        if (!loaded[fieldId]) {
            parseDoubleColumn(fieldId);
        }
        return doubles[fieldId];
    }

    private void parseDoubleColumn(int column)
    {
        loaded[column] = true;

        Object fieldValue = rowData.getFieldValueForThriftId(thriftIds[column]);

        if (fieldValue == null) {
            nulls[column] = true;
        }
        else {
            doubles[column] = ((Number) fieldValue).doubleValue();
            nulls[column] = false;
        }
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
        if (!loaded[fieldId]) {
            parseStringColumn(fieldId);
        }
        return slices[fieldId];
    }

    private void parseStringColumn(int column)
    {
        loaded[column] = true;

        Object fieldValue = rowData.getFieldValueForThriftId(thriftIds[column]);

        if (fieldValue == null) {
            nulls[column] = true;
        }
        else {
            slices[column] = getSliceExpressedValue(fieldValue, types[column]);
            nulls[column] = false;
        }
    }

    private static Slice getSliceExpressedValue(Object value, Type type)
    {
        Slice sliceValue;
        if (value instanceof String) {
            sliceValue = Slices.utf8Slice((String) value);
        }
        else if (value instanceof byte[]) {
            sliceValue = Slices.wrappedBuffer((byte[]) value);
        }
        else if (value instanceof HiveVarchar) {
            sliceValue = Slices.utf8Slice(((HiveVarchar) value).getValue());
        }
        else if (value instanceof HiveChar) {
            sliceValue = Slices.utf8Slice(((HiveChar) value).getValue());
        }
        else if (value instanceof Integer) {
            sliceValue = Slices.utf8Slice(value.toString());
        }
        else {
            throw new IllegalStateException("unsupported string field type: " + value.getClass().getName());
        }
        if (isVarcharType(type)) {
            sliceValue = truncateToLength(sliceValue, type);
        }
        if (isCharType(type)) {
            sliceValue = truncateToLengthAndTrimSpaces(sliceValue, type);
        }

        return sliceValue;
    }

    private void parseDecimalColumn(int column)
    {
        loaded[column] = true;

        Object fieldValue = rowData.getFieldValueForThriftId(thriftIds[column]);

        if (fieldValue == null) {
            nulls[column] = true;
        }
        else {
            HiveDecimal decimal = (HiveDecimal) fieldValue;
            DecimalType columnType = (DecimalType) types[column];
            BigInteger unscaledDecimal = rescale(decimal.unscaledValue(), decimal.scale(), columnType.getScale());

            if (columnType.isShort()) {
                longs[column] = unscaledDecimal.longValue();
            }
            else {
                slices[column] = Decimals.encodeUnscaledValue(unscaledDecimal);
            }
            nulls[column] = false;
        }
    }

    @Override
    public Object getObject(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Block.class);
        if (!loaded[fieldId]) {
            parseObjectColumn(fieldId);
        }
        return objects[fieldId];
    }

    private void parseObjectColumn(int column)
    {
        loaded[column] = true;

        Object fieldValue = rowData.getFieldValueForThriftId(thriftIds[column]);

        if (fieldValue == null) {
            nulls[column] = true;
        }
        else {
            ThriftFieldIdResolver resolver = thriftFieldIdResolver.getNestedResolver(hiveIndexs[column]);
            objects[column] = getBlockObject(types[column], resolver, fieldValue, hiveStorageTimeZone);
            nulls[column] = false;
        }
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        if (!loaded[fieldId]) {
            parseColumn(fieldId);
        }
        return nulls[fieldId];
    }

    private void parseColumn(int column)
    {
        Type type = types[column];
        if (BOOLEAN.equals(type)) {
            parseBooleanColumn(column);
        }
        else if (BIGINT.equals(type)) {
            parseLongColumn(column);
        }
        else if (INTEGER.equals(type)) {
            parseLongColumn(column);
        }
        else if (SMALLINT.equals(type)) {
            parseLongColumn(column);
        }
        else if (TINYINT.equals(type)) {
            parseLongColumn(column);
        }
        else if (REAL.equals(type)) {
            parseLongColumn(column);
        }
        else if (DOUBLE.equals(type)) {
            parseDoubleColumn(column);
        }
        else if (isVarcharType(type) || VARBINARY.equals(type)) {
            parseStringColumn(column);
        }
        else if (isCharType(type)) {
            parseStringColumn(column);
        }
        else if (isStructuralType(hiveTypes[column])) {
            parseObjectColumn(column);
        }
        else if (DATE.equals(type)) {
            parseLongColumn(column);
        }
        else if (TIMESTAMP.equals(type)) {
            parseLongColumn(column);
        }
        else if (type instanceof DecimalType) {
            parseDecimalColumn(column);
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + type);
        }
    }

    private void validateType(int fieldId, Class<?> type)
    {
        if (!types[fieldId].getJavaType().equals(type)) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", type, types[fieldId], fieldId));
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        updateCompletedBytes();

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Error closing thrift record reader", e);
        }
    }

    private static Block getBlockObject(Type type, ThriftFieldIdResolver resolver, Object object, DateTimeZone hiveStorageTimeZone)
    {
        return requireNonNull(serializeObject(type, resolver, null, object, hiveStorageTimeZone), "serialized result is null");
    }

    private static Block serializeObject(Type type, ThriftFieldIdResolver resolver, BlockBuilder builder, Object object, DateTimeZone hiveStorageTimeZone)
    {
        if (object == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }
        if (!isStructuralType(type)) {
            serializePrimitive(type, resolver, builder, object, hiveStorageTimeZone);
            return null;
        }
        else if (isArrayType(type)) {
            return serializeList(type, resolver, builder, object, hiveStorageTimeZone);
        }
        else if (isMapType(type)) {
            return serializeMap(type, resolver, builder, object, hiveStorageTimeZone);
        }
        else if (isRowType(type)) {
            return serializeStruct(type, resolver, builder, object, hiveStorageTimeZone);
        }
        throw new RuntimeException("Unknown object type: " + type);
    }

    private static Block serializeList(Type type, ThriftFieldIdResolver resolver, BlockBuilder builder, Object object, DateTimeZone hiveStorageTimeZone)
    {
        List<?> list = (List) requireNonNull(object, "object is null");
        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "list must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);
        ThriftFieldIdResolver elementResolver = resolver.getNestedResolver(0);
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = elementType.createBlockBuilder(null, list.size());
        }

        for (Object element : list) {
            serializeObject(elementType, elementResolver, currentBuilder, element, hiveStorageTimeZone);
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeMap(Type type, ThriftFieldIdResolver resolver, BlockBuilder builder, Object object, DateTimeZone hiveStorageTimeZone)
    {
        Map<?, ?> map = (Map) requireNonNull(object, "object is null");
        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 2, "map must have exactly 2 type parameter");
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);
        ThriftFieldIdResolver keyResolver = resolver.getNestedResolver(0);
        ThriftFieldIdResolver valueResolver = resolver.getNestedResolver(1);
        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(null, 1);
        }
        BlockBuilder currentBuilder = builder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyType, keyResolver, currentBuilder, entry.getKey(), hiveStorageTimeZone);
                serializeObject(valueType, valueResolver, currentBuilder, entry.getValue(), hiveStorageTimeZone);
            }
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        }
        else {
            return null;
        }
    }

    private static Block serializeStruct(Type type, ThriftFieldIdResolver resolver, BlockBuilder builder, Object object, DateTimeZone hiveStorageTimeZone)
    {
        ThriftGenericRow structData = (ThriftGenericRow) requireNonNull(object, "object is null");
        List<Type> typeParameters = type.getTypeParameters();

        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(null, 1);
        }
        BlockBuilder currentBuilder = builder.beginBlockEntry();

        for (int i = 0; i < typeParameters.size(); i++) {
            Object fieldValue = structData.getFieldValueForThriftId(getThriftIdWithFailOver(resolver, i));
            if (fieldValue == null) {
                currentBuilder.appendNull();
            }
            else {
                serializeObject(typeParameters.get(i), resolver.getNestedResolver(i), currentBuilder, fieldValue, hiveStorageTimeZone);
            }
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        }
        else {
            return null;
        }
    }

    private static void serializePrimitive(Type type, ThriftFieldIdResolver resolver, BlockBuilder builder, Object object, DateTimeZone hiveStorageTimeZone)
    {
        requireNonNull(builder, "parent builder is null");
        requireNonNull(object, "object is null");

        if (BOOLEAN.equals(type)) {
            BOOLEAN.writeBoolean(builder, (Boolean) object);
        }
        else if (BIGINT.equals(type)) {
            BIGINT.writeLong(builder, getLongExpressedValue(object, hiveStorageTimeZone));
        }
        else if (INTEGER.equals(type)) {
            INTEGER.writeLong(builder, getLongExpressedValue(object, hiveStorageTimeZone));
        }
        else if (SMALLINT.equals(type)) {
            SMALLINT.writeLong(builder, getLongExpressedValue(object, hiveStorageTimeZone));
        }
        else if (TINYINT.equals(type)) {
            TINYINT.writeLong(builder, getLongExpressedValue(object, hiveStorageTimeZone));
        }
        else if (REAL.equals(type)) {
            REAL.writeLong(builder, getLongExpressedValue(object, hiveStorageTimeZone));
        }
        else if (DOUBLE.equals(type)) {
            DOUBLE.writeDouble(builder, ((Number) object).doubleValue());
        }
        else if (isVarcharType(type) || VARBINARY.equals(type) || isCharType(type)) {
            type.writeSlice(builder, getSliceExpressedValue(object, type));
        }
        else if (DATE.equals(type)) {
            DATE.writeLong(builder, getLongExpressedValue(object, hiveStorageTimeZone));
        }
        else if (TIMESTAMP.equals(type)) {
            TIMESTAMP.writeLong(builder, getLongExpressedValue(object, hiveStorageTimeZone));
        }
        else if (type instanceof DecimalType) {
            HiveDecimal decimal = (HiveDecimal) object;
            DecimalType decimalType = (DecimalType) type;
            BigInteger unscaledDecimal = rescale(decimal.unscaledValue(), decimal.scale(), decimalType.getScale());
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, unscaledDecimal.longValue());
            }
            else {
                decimalType.writeSlice(builder, Decimals.encodeUnscaledValue(unscaledDecimal));
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported primitive type: " + type);
        }
    }

    private static short getThriftIdWithFailOver(ThriftFieldIdResolver thriftFieldIdResolver, int hiveIndex)
    {
        try {
            return thriftFieldIdResolver.getThriftId(hiveIndex);
        }
        catch (PrestoException e) {
            return NON_EXISTED_THRIFT_ID;
        }
    }
}
