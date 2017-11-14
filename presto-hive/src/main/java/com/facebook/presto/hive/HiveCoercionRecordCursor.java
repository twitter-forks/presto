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
package com.facebook.presto.hive;

import com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveCoercionRecordCursor
        implements RecordCursor
{
    private final RecordCursor delegate;
    private final List<ColumnMapping> columnMappings;
    private final Coercer[] coercers;

    public HiveCoercionRecordCursor(
            List<ColumnMapping> columnMappings,
            TypeManager typeManager,
            RecordCursor delegate)
    {
        requireNonNull(columnMappings, "columns is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = ImmutableList.copyOf(columnMappings);

        int size = columnMappings.size();

        this.coercers = new Coercer[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);

            if (columnMapping.getCoercionFrom().isPresent()) {
                coercers[columnIndex] = createCoercer(typeManager, columnMapping.getCoercionFrom().get(), columnMapping.getHiveColumnHandle().getHiveType());
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public Type getType(int field)
    {
        return delegate.getType(field);
    }

    @Override
    public boolean advanceNextPosition()
    {
        for (int i = 0; i < columnMappings.size(); i++) {
            if (coercers[i] != null) {
                coercers[i].reset();
            }
        }
        return delegate.advanceNextPosition();
    }

    @Override
    public boolean getBoolean(int field)
    {
        if (coercers[field] == null) {
            return delegate.getBoolean(field);
        }
        return coercers[field].getBoolean(delegate, field);
    }

    @Override
    public long getLong(int field)
    {
        if (coercers[field] == null) {
            return delegate.getLong(field);
        }
        return coercers[field].getLong(delegate, field);
    }

    @Override
    public double getDouble(int field)
    {
        if (coercers[field] == null) {
            return delegate.getDouble(field);
        }
        return coercers[field].getDouble(delegate, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        if (coercers[field] == null) {
            return delegate.getSlice(field);
        }
        return coercers[field].getSlice(delegate, field);
    }

    @Override
    public Object getObject(int field)
    {
        if (coercers[field] == null) {
            return delegate.getObject(field);
        }
        return coercers[field].getObject(delegate, field);
    }

    @Override
    public boolean isNull(int field)
    {
        if (coercers[field] == null) {
            return delegate.isNull(field);
        }
        return coercers[field].isNull(delegate, field);
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @VisibleForTesting
    RecordCursor getRegularColumnRecordCursor()
    {
        return delegate;
    }

    private abstract static class Coercer
    {
        private boolean isNull;
        private boolean loaded;

        private boolean booleanValue;
        private long longValue;
        private double doubleValue;
        private Slice sliceValue;
        private Object objectValue;

        public void reset()
        {
            isNull = false;
            loaded = false;
        }

        public boolean isNull(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return isNull;
        }

        public boolean getBoolean(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return booleanValue;
        }

        public long getLong(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return longValue;
        }

        public double getDouble(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return doubleValue;
        }

        public Slice getSlice(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return sliceValue;
        }

        public Object getObject(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return objectValue;
        }

        private void assureLoaded(RecordCursor delegate, int field)
        {
            if (!loaded) {
                isNull = delegate.isNull(field);
                if (!isNull) {
                    coerce(delegate, field);
                }
                loaded = true;
            }
        }

        protected abstract void coerce(RecordCursor delegate, int field);

        protected void setBoolean(boolean value)
        {
            booleanValue = value;
        }

        protected void setLong(long value)
        {
            longValue = value;
        }

        protected void setDouble(double value)
        {
            doubleValue = value;
        }

        protected void setSlice(Slice value)
        {
            sliceValue = value;
        }

        protected void setObject(Object value)
        {
            objectValue = value;
        }

        protected void setIsNull(boolean isNull)
        {
            this.isNull = isNull;
        }
    }

    private static Coercer createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return new IntegerNumberToVarcharCoercer();
        }
        else if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return new VarcharToIntegerNumberCoercer(toHiveType);
        }
        else if (fromHiveType.equals(HIVE_BYTE) && toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer();
        }
        else if (fromHiveType.equals(HIVE_SHORT) && toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer();
        }
        else if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer();
        }
        else if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return new FloatToDoubleCoercer();
        }
        else if (HiveUtil.isArrayType(fromType) && HiveUtil.isArrayType(toType)) {
            return new ListToListCoercer(typeManager, fromHiveType, toHiveType);
        }
        else if (HiveUtil.isMapType(fromType) && HiveUtil.isMapType(toType)) {
            return new MapToMapCoercer(typeManager, fromHiveType, toHiveType);
        }
        else if (HiveUtil.isRowType(fromType) && HiveUtil.isRowType(toType)) {
            return new StructToStructCoercer(typeManager, fromHiveType, toHiveType);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }

    private static class IntegerNumberUpscaleCoercer
            extends Coercer
    {
        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            setLong(delegate.getLong(field));
        }
    }

    private static class IntegerNumberToVarcharCoercer
            extends Coercer
    {
        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            setSlice(utf8Slice(String.valueOf(delegate.getLong(field))));
        }
    }

    private static class FloatToDoubleCoercer
            extends Coercer
    {
        @Override
        protected void coerce(RecordCursor delegate, int field)
        {
            setDouble(intBitsToFloat((int) delegate.getLong(field)));
        }
    }

    private static class VarcharToIntegerNumberCoercer
            extends Coercer
    {
        private final long maxValue;
        private final long minValue;

        public VarcharToIntegerNumberCoercer(HiveType type)
        {
            if (type.equals(HIVE_BYTE)) {
                minValue = Byte.MIN_VALUE;
                maxValue = Byte.MAX_VALUE;
            }
            else if (type.equals(HIVE_SHORT)) {
                minValue = Short.MIN_VALUE;
                maxValue = Short.MAX_VALUE;
            }
            else if (type.equals(HIVE_INT)) {
                minValue = Integer.MIN_VALUE;
                maxValue = Integer.MAX_VALUE;
            }
            else if (type.equals(HIVE_LONG)) {
                minValue = Long.MIN_VALUE;
                maxValue = Long.MAX_VALUE;
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format("Could not create Coercer from varchar to %s", type));
            }
        }

        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            try {
                long value = Long.parseLong(delegate.getSlice(field).toStringUtf8());
                if (minValue <= value && value <= maxValue) {
                    setLong(value);
                }
                else {
                    setIsNull(true);
                }
            }
            catch (NumberFormatException e) {
                setIsNull(true);
            }
        }
    }

    private static class ListToListCoercer
            extends Coercer
    {
        private final TypeManager typeManager;
        private final HiveType fromHiveType;
        private final HiveType toHiveType;
        private final HiveType fromElementHiveType;
        private final HiveType toElementHiveType;
        private final Coercer elementCoercer;

        public ListToListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            this.typeManager = requireNonNull(typeManager, "typeManage is null");
            this.fromHiveType = requireNonNull(fromHiveType, "fromHiveType is null");
            this.toHiveType = requireNonNull(toHiveType, "toHiveType is null");
            this.fromElementHiveType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            this.toElementHiveType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            this.elementCoercer = fromElementHiveType.equals(toElementHiveType) ? null : createCoercer(typeManager, fromElementHiveType, toElementHiveType);
        }

        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            if (delegate.isNull(field)) {
                setIsNull(true);
                return;
            }
            Block block = (Block) delegate.getObject(field);
            BlockBuilder builder = toHiveType.getType(typeManager).createBlockBuilder(new BlockBuilderStatus(), 1);
            BlockBuilder listBuilder = builder.beginBlockEntry();
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    listBuilder.appendNull();
                }
                else if (elementCoercer == null) {
                    block.writePositionTo(i, listBuilder);
                    listBuilder.closeEntry();
                }
                else {
                    rewriteBlock(fromElementHiveType, toElementHiveType, block, i, listBuilder, elementCoercer, typeManager, field);
                }
            }
            builder.closeEntry();
            setObject(builder.build().getObject(0, Block.class));
        }
    }

    private static class MapToMapCoercer
            extends Coercer
    {
        private final TypeManager typeManager;
        private final HiveType fromHiveType;
        private final HiveType toHiveType;
        private final HiveType fromKeyHiveType;
        private final HiveType toKeyHiveType;
        private final HiveType fromValueHiveType;
        private final HiveType toValueHiveType;
        private final Coercer keyCoercer;
        private final Coercer valueCoercer;

        public MapToMapCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            this.typeManager = requireNonNull(typeManager, "typeManage is null");
            this.fromHiveType = requireNonNull(fromHiveType, "fromHiveType is null");
            this.toHiveType = requireNonNull(toHiveType, "toHiveType is null");
            this.fromKeyHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            this.fromValueHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            this.toKeyHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            this.toValueHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            this.keyCoercer = fromKeyHiveType.equals(toKeyHiveType) ? null : createCoercer(typeManager, fromKeyHiveType, toKeyHiveType);
            this.valueCoercer = fromValueHiveType.equals(toValueHiveType) ? null : createCoercer(typeManager, fromValueHiveType, toValueHiveType);
        }

        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            if (delegate.isNull(field)) {
                setIsNull(true);
                return;
            }
            Block block = (Block) delegate.getObject(field);
            BlockBuilder builder = toHiveType.getType(typeManager).createBlockBuilder(new BlockBuilderStatus(), 1);
            BlockBuilder mapBuilder = builder.beginBlockEntry();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                if (block.isNull(i)) {
                    mapBuilder.appendNull();
                }
                else if (keyCoercer == null) {
                    block.writePositionTo(i, mapBuilder);
                    mapBuilder.closeEntry();
                }
                else {
                    rewriteBlock(fromKeyHiveType, toKeyHiveType, block.getSingleValueBlock(i), 0, mapBuilder, keyCoercer, typeManager, field);
                }
                if (block.isNull(i + 1)) {
                    mapBuilder.appendNull();
                }
                if (valueCoercer == null) {
                    block.writePositionTo(i + 1, mapBuilder);
                    mapBuilder.closeEntry();
                }
                else {
                    rewriteBlock(fromValueHiveType, toValueHiveType, block.getSingleValueBlock(i + 1), 0, mapBuilder, valueCoercer, typeManager, field);
                }
            }
            builder.closeEntry();
            setObject(builder.build().getObject(0, Block.class));
        }
    }

    private static class StructToStructCoercer
            extends Coercer
    {
        private final TypeManager typeManager;
        private final HiveType fromHiveType;
        private final HiveType toHiveType;
        private final List<HiveType> fromFieldTypes;
        private final List<HiveType> toFieldTypes;
        private final Coercer[] coercers;

        public StructToStructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            this.typeManager = requireNonNull(typeManager, "typeManage is null");
            this.fromHiveType = requireNonNull(fromHiveType, "fromHiveType is null");
            this.toHiveType = requireNonNull(toHiveType, "toHiveType is null");
            this.fromFieldTypes = getAllStructFieldTypeInfos(fromHiveType);
            this.toFieldTypes = getAllStructFieldTypeInfos(toHiveType);
            this.coercers = new Coercer[toFieldTypes.size()];
            Arrays.fill(this.coercers, null);
            for (int i = 0; i < Math.min(fromFieldTypes.size(), toFieldTypes.size()); i++) {
                if (!fromFieldTypes.get(i).equals(toFieldTypes.get(i))) {
                    coercers[i] = createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i));
                }
            }
        }

        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            if (delegate.isNull(field)) {
                setIsNull(true);
                return;
            }
            Block block = (Block) delegate.getObject(field);
            BlockBuilder builder = toHiveType.getType(typeManager).createBlockBuilder(new BlockBuilderStatus(), 1);
            BlockBuilder rowBuilder = builder.beginBlockEntry();
            for (int i = 0; i < toFieldTypes.size(); i++) {
                if (i >= fromFieldTypes.size() || block.isNull(i)) {
                    rowBuilder.appendNull();
                }
                else if (coercers[i] == null) {
                    block.writePositionTo(i, rowBuilder);
                    rowBuilder.closeEntry();
                }
                else {
                    rewriteBlock(fromFieldTypes.get(i), toFieldTypes.get(i), block, i, rowBuilder, coercers[i], typeManager, field);
                }
            }
            builder.closeEntry();
            setObject(builder.build().getObject(0, Block.class));
        }
    }

    private static void rewriteBlock(
            HiveType fromFieldHiveType,
            HiveType toFieldHiveType,
            Block block,
            int position,
            BlockBuilder builder,
            Coercer coercer,
            TypeManager typeManager,
            int field)
    {
        Type fromFieldType = fromFieldHiveType.getType(typeManager);
        Type toFieldType = toFieldHiveType.getType(typeManager);
        Object value = null;
        if (fromFieldHiveType.equals(HIVE_BYTE) || fromFieldHiveType.equals(HIVE_SHORT) || fromFieldHiveType.equals(HIVE_INT) || fromFieldHiveType.equals(HIVE_LONG) || fromFieldHiveType.equals(HIVE_FLOAT)) {
            value = fromFieldType.getLong(block, position);
        }
        else if (fromFieldType instanceof VarcharType) {
            value = fromFieldType.getSlice(block, position);
        }
        else if (HiveUtil.isStructuralType(fromFieldHiveType)) {
            value = fromFieldType.getObject(block, position);
        }
        coercer.reset();
        RecordCursor bridgingRecordCursor = createBridgingRecordCursor(value, typeManager, fromFieldHiveType);
        if (coercer.isNull(bridgingRecordCursor, field)) {
            builder.appendNull();
        }
        else if (toFieldHiveType.equals(HIVE_BYTE) || toFieldHiveType.equals(HIVE_SHORT) || toFieldHiveType.equals(HIVE_INT) || toFieldHiveType.equals(HIVE_LONG) || toFieldHiveType.equals(HIVE_FLOAT)) {
            toFieldType.writeLong(builder, coercer.getLong(bridgingRecordCursor, field));
        }
        else if (toFieldHiveType.equals(HIVE_DOUBLE)) {
            toFieldType.writeDouble(builder, coercer.getDouble(bridgingRecordCursor, field));
        }
        else if (toFieldType instanceof VarcharType) {
            toFieldType.writeSlice(builder, coercer.getSlice(bridgingRecordCursor, field));
        }
        else if (HiveUtil.isStructuralType(toFieldHiveType)) {
            toFieldType.writeObject(builder, coercer.getObject(bridgingRecordCursor, field));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromFieldHiveType, toFieldHiveType));
        }
        coercer.reset();
    }

    private static RecordCursor createBridgingRecordCursor(
            Object value,
            TypeManager typeManager,
            HiveType hiveType)
    {
        return new RecordCursor() {
            @Override
            public long getCompletedBytes()
            {
                return 0;
            }

            @Override
            public long getReadTimeNanos()
            {
                return 0;
            }

            @Override
            public Type getType(int field)
            {
                return hiveType.getType(typeManager);
            }

            @Override
            public boolean advanceNextPosition()
            {
                return true;
            }

            @Override
            public boolean getBoolean(int field)
            {
                return (Boolean) value;
            }

            @Override
            public long getLong(int field)
            {
                return (Long) value;
            }

            @Override
            public double getDouble(int field)
            {
                return (Double) value;
            }

            @Override
            public Slice getSlice(int field)
            {
                return (Slice) value;
            }

            @Override
            public Object getObject(int field)
            {
                return value;
            }

            @Override
            public boolean isNull(int field)
            {
                return Objects.isNull(value);
            }

            @Override
            public void close()
            {
            }
        };
    }

    private static List<HiveType> getAllStructFieldTypeInfos(HiveType hiveType)
    {
        return ((StructTypeInfo) hiveType.getTypeInfo()).getAllStructFieldTypeInfos()
            .stream().map(typeInfo -> HiveType.valueOf(typeInfo.getTypeName())).collect(Collectors.toList());
    }
}
