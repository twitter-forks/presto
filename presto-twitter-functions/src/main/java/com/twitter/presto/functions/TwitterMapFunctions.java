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
package com.twitter.presto.functions;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;

public class TwitterMapFunctions
{
    private TwitterMapFunctions()
    {
    }

    @ScalarFunction("key_of_max_value")
    @Description("Get the key of the entry of map that holding max value. If more than one entry holds the same max value, return null")
    @TypeParameter("K")
    @SqlType("K")
    @SqlNullable
    public static Object keyMaxValue(@TypeParameter("K") Type keyType, @SqlType("map(K,bigint)") Block map)
    {
        Object keyOfMaxValue = null;
        long maxValue = Long.MIN_VALUE;
        for (int position = 0; position < map.getPositionCount(); position += 2) {
            Object key = readNativeValue(keyType, map, position);
            long value = BIGINT.getLong(map, position + 1);
            if (value > maxValue) {
                keyOfMaxValue = key;
                maxValue = value;
            }
            else if (value == maxValue) {
                keyOfMaxValue = null;
            }
        }

        return keyOfMaxValue;
    }
}
