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
package com.facebook.presto.hive.parquet.reader;

import com.facebook.presto.hive.parquet.Field;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntList;

import static com.facebook.presto.hive.parquet.ParquetTypeUtils.isValueNull;

public class ParquetStructColumnReader
{
    private ParquetStructColumnReader()
    {
    }

    /**
     * Each struct has three variants of presence:
     * 1) Struct is not defined, because one of it's optional parent fields is null
     * 2) Struct is null
     * 3) Struct is defined and not empty. In this case offset value is increased by one.
     * <p>
     * One of the struct's field repetition/definition levels are used to calculate offsets of the struct.
     */
    public static void calculateStructOffsets(
            Field field,
            IntList structOffsets,
            BooleanList structIsNull,
            int[] fieldDefinitionLevels,
            int[] fieldRepetitionLevels)
    {
        int maxDefinitionLevel = field.getDefinitionLevel();
        int maxRepetitionLevel = field.getRepetitionLevel();
        boolean required = field.isRequired();
        int offset = 0;
        structOffsets.add(offset);
        if (fieldDefinitionLevels == null) {
            return;
        }
        for (int i = 0; i < fieldDefinitionLevels.length; i++) {
            if (fieldRepetitionLevels[i] <= maxRepetitionLevel) {
                if (isValueNull(required, fieldDefinitionLevels[i], maxDefinitionLevel)) {
                    // Struct is null
                    structIsNull.add(true);
                    structOffsets.add(offset);
                }
                else if (fieldDefinitionLevels[i] >= maxDefinitionLevel) {
                    // Struct is defined and not empty
                    structIsNull.add(false);
                    offset++;
                    structOffsets.add(offset);
                }
            }
        }
    }
}
