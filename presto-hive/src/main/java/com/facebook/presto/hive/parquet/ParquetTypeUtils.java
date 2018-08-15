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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import parquet.column.Encoding;
import parquet.io.ColumnIO;
import parquet.io.ColumnIOFactory;
import parquet.io.GroupColumnIO;
import parquet.io.InvalidRecordException;
import parquet.io.MessageColumnIO;
import parquet.io.ParquetDecodingException;
import parquet.io.PrimitiveColumnIO;
import parquet.schema.DecimalMetadata;
import parquet.schema.MessageType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Optional.empty;
import static parquet.schema.OriginalType.DECIMAL;
import static parquet.schema.Type.Repetition.REPEATED;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils()
    {
    }

    public static List<PrimitiveColumnIO> getColumns(MessageType fileSchema, MessageType requestedSchema)
    {
        return (new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true).getLeaves();
    }

    public static MessageColumnIO getColumnIO(MessageType fileSchema, MessageType requestedSchema)
    {
        return (new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true);
    }

    public static GroupColumnIO getMapKeyValueColumn(GroupColumnIO groupColumnIO)
    {
        while (groupColumnIO.getChildrenCount() == 1) {
            groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
        }
        return groupColumnIO;
    }

    /* For backward-compatibility, the type of elements in LIST-annotated structures should always be determined by the following rules:
     * 1. If the repeated field is not a group, then its type is the element type and elements are required.
     * 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
     * 3. If the repeated field is a group with one field and is named either array or uses the LIST-annotated group's name with _tuple appended then the repeated type is the element type and elements are required.
     * 4. Otherwise, the repeated field's type is the element type with the repeated field's repetition.
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     */
    public static ColumnIO getArrayElementColumn(ColumnIO columnIO)
    {
        while (columnIO instanceof GroupColumnIO && !columnIO.getType().isRepetition(REPEATED)) {
            columnIO = ((GroupColumnIO) columnIO).getChild(0);
        }

        /* If array has a standard 3-level structure with middle level repeated group with a single field:
         *  optional group my_list (LIST) {
         *     repeated group element {
         *        required binary str (UTF8);
         *     };
         *  }
         */
        if (columnIO instanceof GroupColumnIO &&
                columnIO.getType().getOriginalType() == null &&
                ((GroupColumnIO) columnIO).getChildrenCount() == 1 &&
                !columnIO.getName().equals("array") &&
                !columnIO.getName().equals(columnIO.getParent().getName() + "_tuple")) {
            return ((GroupColumnIO) columnIO).getChild(0);
        }

        /* Backward-compatibility support for 2-level arrays where a repeated field is not a group:
         *   optional group my_list (LIST) {
         *      repeated int32 element;
         *   }
         */
        return columnIO;
    }

    public static Map<List<String>, RichColumnDescriptor> getDescriptors(MessageType fileSchema, MessageType requestedSchema)
    {
        Map<List<String>, RichColumnDescriptor> descriptorsByPath = new HashMap<>();
        List<PrimitiveColumnIO> columns = getColumns(fileSchema, requestedSchema);
        for (String[] paths : fileSchema.getPaths()) {
            List<String> columnPath = Arrays.asList(paths);
            getDescriptor(columns, columnPath)
                    .ifPresent(richColumnDescriptor -> descriptorsByPath.put(columnPath, richColumnDescriptor));
        }
        return descriptorsByPath;
    }

    public static Optional<RichColumnDescriptor> getDescriptor(List<PrimitiveColumnIO> columns, List<String> path)
    {
        checkArgument(path.size() >= 1, "Parquet nested path should have at least one component");
        int index = getPathIndex(columns, path);
        if (index == -1) {
            return empty();
        }
        PrimitiveColumnIO columnIO = columns.get(index);
        return Optional.of(new RichColumnDescriptor(columnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType()));
    }

    private static int getPathIndex(List<PrimitiveColumnIO> columns, List<String> path)
    {
        int maxLevel = path.size();
        int index = -1;
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ColumnIO[] fields = columns.get(columnIndex).getPath();
            if (fields.length <= maxLevel) {
                continue;
            }
            if (fields[maxLevel].getName().equalsIgnoreCase(path.get(maxLevel - 1))) {
                boolean match = true;
                for (int level = 0; level < maxLevel - 1; level++) {
                    if (!fields[level + 1].getName().equalsIgnoreCase(path.get(level))) {
                        match = false;
                    }
                }

                if (match) {
                    index = columnIndex;
                }
            }
        }
        return index;
    }

    public static Type getPrestoType(RichColumnDescriptor descriptor)
    {
        switch (descriptor.getType()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BINARY:
                return createDecimalType(descriptor).orElse(VarcharType.VARCHAR);
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case INT32:
                return createDecimalType(descriptor).orElse(IntegerType.INTEGER);
            case INT64:
                return createDecimalType(descriptor).orElse(BigintType.BIGINT);
            case INT96:
                return TimestampType.TIMESTAMP;
            case FIXED_LEN_BYTE_ARRAY:
                return createDecimalType(descriptor).orElseThrow(() -> new PrestoException(NOT_SUPPORTED, "Parquet type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType()));
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }

    public static int getFieldIndex(MessageType fileSchema, String name)
    {
        try {
            return fileSchema.getFieldIndex(name.toLowerCase());
        }
        catch (InvalidRecordException e) {
            for (parquet.schema.Type type : fileSchema.getFields()) {
                if (type.getName().equalsIgnoreCase(name)) {
                    return fileSchema.getFieldIndex(type.getName());
                }
            }
            return -1;
        }
    }

    public static parquet.schema.Type getPrunedParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames, boolean pruneNestedFields)
    {
        parquet.schema.Type originalType = getParquetType(column, messageType, useParquetColumnNames);
        if (pruneNestedFields && column.getFieldSet().isPresent()) {
            return pruneParquetType(originalType, column.getFieldSet().get());
        }

        return originalType;
    }

    private static parquet.schema.Type pruneParquetType(parquet.schema.Type type, Set<String> requiredFields)
    {
        if (requiredFields.isEmpty()) {
            return type;
        }

        if (type.isPrimitive()) {
            return type;
        }

        Map<String, Set<String>> fields = groupFields(requiredFields);

        List<parquet.schema.Type> newFields = fields.entrySet().stream()
                .map(entry -> pruneParquetType(type.asGroupType().getType(entry.getKey()), entry.getValue()))
                .collect(toImmutableList());

        return type.asGroupType().withNewFields(newFields);
    }

    private static Map<String, Set<String>> groupFields(Set<String> requiredFields)
    {
        Map<String, Set<String>> fields = new HashMap<>();
        for (String field : requiredFields) {
            String[] path = field.split("\\.", 2);
            String fieldName = path[0];
            Set<String> nestedField = path.length == 1 ? ImmutableSet.of() : ImmutableSet.of(path[1]);
            if (fields.containsKey(fieldName)) {
                fields.get(fieldName).addAll(nestedField);
            }
            else {
                fields.put(fieldName, new HashSet<>(nestedField));
            }
        }

        return ImmutableMap.copyOf(fields);
    }

    public static parquet.schema.Type getParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return findParquetTypeByName(column, messageType);
        }

        if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
            return messageType.getType(column.getHiveColumnIndex());
        }
        return null;
    }

    /**
     * Find the column type by name using returning the first match with the following logic:
     * <ul>
     * <li>direct match</li>
     * <li>case-insensitive match</li>
     * <li>if the name ends with _, remove it and direct match</li>
     * <li>if the name ends with _, remove it and case-insensitive match</li>
     * </ul>
     */
    private static parquet.schema.Type findParquetTypeByName(HiveColumnHandle column, MessageType messageType)
    {
        String name = column.getName();
        parquet.schema.Type type = getParquetTypeByName(name, messageType);

        // when a parquet field is a hive keyword we append an _ to it in hive. When doing
        // a name-based lookup, we need to strip it off again if we didn't get a direct match.
        if (type == null && name.endsWith("_")) {
            type = getParquetTypeByName(name.substring(0, name.length() - 1), messageType);
        }
        return type;
    }

    // Find the column index by name following the same logic as findParquetTypeByName
    public static int findFieldIndexByName(MessageType fileSchema, String name)
    {
        // direct match and case-insensitive match
        int fieldIndex = getFieldIndex(fileSchema, name);

        // when a parquet field is a hive keyword we append an _ to it in hive.
        // try remove _ and direct match / case-insensitive match again
        if (fieldIndex == -1 && name.endsWith("_")) {
            fieldIndex = getFieldIndex(fileSchema, name.substring(0, name.length() - 1));
        }

        return fieldIndex;
    }

    // Find the ColumnIO by name following the same logic as findParquetTypeByName
    public static ColumnIO findColumnIObyName(GroupColumnIO groupColumnIO, String name)
    {
        // direct match and case-insensitive match
        ColumnIO columnIO = getColumnIOByName(groupColumnIO, name);

        if (columnIO == null && name.endsWith("_")) {
            return findColumnIObyName(groupColumnIO, name.substring(0, name.length() - 1));
        }

        return columnIO;
    }

    public static ParquetEncoding getParquetEncoding(Encoding encoding)
    {
        switch (encoding) {
            case PLAIN:
                return ParquetEncoding.PLAIN;
            case RLE:
                return ParquetEncoding.RLE;
            case BIT_PACKED:
                return ParquetEncoding.BIT_PACKED;
            case PLAIN_DICTIONARY:
                return ParquetEncoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED:
                return ParquetEncoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY:
                return ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY:
                return ParquetEncoding.DELTA_BYTE_ARRAY;
            case RLE_DICTIONARY:
                return ParquetEncoding.RLE_DICTIONARY;
            default:
                throw new ParquetDecodingException("Unsupported Parquet encoding: " + encoding);
        }
    }

    private static parquet.schema.Type getParquetTypeByName(String columnName, MessageType messageType)
    {
        if (messageType.containsField(columnName)) {
            return messageType.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (parquet.schema.Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }

    private static ColumnIO getColumnIOByName(GroupColumnIO groupColumnIO, String name)
    {
        ColumnIO columnIO = groupColumnIO.getChild(name);

        if (columnIO != null) {
            return columnIO;
        }

        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getName().equalsIgnoreCase(name)) {
                return groupColumnIO.getChild(i);
            }
        }

        return null;
    }

    public static Optional<Type> createDecimalType(RichColumnDescriptor descriptor)
    {
        if (descriptor.getPrimitiveType().getOriginalType() != DECIMAL) {
            return Optional.empty();
        }
        DecimalMetadata decimalMetadata = descriptor.getPrimitiveType().getDecimalMetadata();
        return Optional.of(DecimalType.createDecimalType(decimalMetadata.getPrecision(), decimalMetadata.getScale()));
    }

    /**
     * For optional fields:
     * definitionLevel == maxDefinitionLevel     => Value is defined
     * definitionLevel == maxDefinitionLevel - 1 => Value is null
     * definitionLevel < maxDefinitionLevel - 1  => Value does not exist, because one of its optional parent fields is null
     */
    public static boolean isValueNull(boolean required, int definitionLevel, int maxDefinitionLevel)
    {
        return !required && (definitionLevel == maxDefinitionLevel - 1);
    }
}
