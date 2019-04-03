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
package com.twitter.presto.parquet;

import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.schema.MessageType;

import static io.prestosql.parquet.ParquetTypeUtils.getFieldIndex;
import static io.prestosql.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.prestosql.parquet.ParquetTypeUtils.lookupColumnByName;

public class ParquetTypeUtils
{
    private ParquetTypeUtils()
    {
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
    public static org.apache.parquet.schema.Type findParquetTypeByName(String name, MessageType messageType)
    {
        org.apache.parquet.schema.Type type = getParquetTypeByName(name, messageType);

        if (type == null && name.endsWith("_")) {
            type = getParquetTypeByName(name.substring(0, name.length() - 1), messageType);
        }
        return type;
    }

    // Find the column index by name following the same logic as findParquetTypeByName
    public static int findFieldIndexByName(MessageType fileSchema, String name)
    {
        int fieldIndex = getFieldIndex(fileSchema, name);

        if (fieldIndex == -1 && name.endsWith("_")) {
            fieldIndex = getFieldIndex(fileSchema, name.substring(0, name.length() - 1));
        }

        return fieldIndex;
    }

    // Find the ColumnIO by name following the same logic as findParquetTypeByName
    public static ColumnIO findColumnIObyName(GroupColumnIO groupColumnIO, String name)
    {
        ColumnIO columnIO = lookupColumnByName(groupColumnIO, name);

        if (columnIO == null && name.endsWith("_")) {
            columnIO = lookupColumnByName(groupColumnIO, name.substring(0, name.length() - 1));
        }

        return columnIO;
    }
}
