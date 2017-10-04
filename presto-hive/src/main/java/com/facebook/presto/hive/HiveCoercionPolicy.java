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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.valueOf;

import static java.util.Objects.requireNonNull;

public class HiveCoercionPolicy
        implements CoercionPolicy
{
    private final TypeManager typeManager;

    @Inject
    public HiveCoercionPolicy(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public boolean canCoerce(HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (fromType instanceof VarcharType) {
            return toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG);
        }
        if (toType instanceof VarcharType) {
            return fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_BYTE)) {
            return toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_SHORT)) {
            return toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_INT)) {
            return toHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_FLOAT)) {
            return toHiveType.equals(HIVE_DOUBLE);
        }

        return canCoerceForList(fromHiveType, toHiveType) || canCoerceForMap(fromHiveType, toHiveType) || canCoerceForStruct(fromHiveType, toHiveType);
    }

    private boolean canCoerceForMap(HiveType fromHiveType, HiveType toHiveType)
    {
        if (!fromHiveType.getCategory().equals(Category.MAP) || !toHiveType.getCategory().equals(Category.MAP)) {
            return false;
        }
        HiveType fromKeyType = valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
        HiveType fromValueType = valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
        HiveType toKeyType = valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
        HiveType toValueType = valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
        return canCoerce(fromKeyType, toKeyType) && canCoerce(fromValueType, toValueType);
    }

    private boolean canCoerceForList(HiveType fromHiveType, HiveType toHiveType)
    {
        if (!fromHiveType.getCategory().equals(Category.LIST) || !toHiveType.getCategory().equals(Category.LIST)) {
            return false;
        }
        HiveType fromElementType = valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
        HiveType toElementType = valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
        return canCoerce(fromElementType, toElementType);
    }

    private boolean canCoerceForStruct(HiveType fromHiveType, HiveType toHiveType)
    {
        if (!fromHiveType.getCategory().equals(Category.STRUCT) || !toHiveType.getCategory().equals(Category.STRUCT)) {
            return false;
        }
        List<HiveType> fromFieldTypes = getAllStructFieldTypeInfos(fromHiveType);
        List<HiveType> toFieldTypes = getAllStructFieldTypeInfos(toHiveType);
        for (int i = 0; i < Math.min(fromFieldTypes.size(), toFieldTypes.size()); i++) {
            if (!canCoerce(fromFieldTypes.get(i), toFieldTypes.get(i))) {
                return false;
            }
        }
        return true;
    }

    private List<HiveType> getAllStructFieldTypeInfos(HiveType hiveType)
    {
        return ((StructTypeInfo) hiveType.getTypeInfo()).getAllStructFieldTypeInfos()
            .stream().map(typeInfo -> valueOf(typeInfo.getTypeName())).collect(Collectors.toList());
    }
}
