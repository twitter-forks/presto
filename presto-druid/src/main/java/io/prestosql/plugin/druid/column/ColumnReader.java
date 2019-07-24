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
package io.prestosql.plugin.druid.column;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.apache.druid.segment.ColumnValueSelector;

import java.io.IOException;

import static io.prestosql.plugin.druid.DruidErrorCode.DRUID_UNSUPPORTED_TYPE_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

// TODO: refactor duplicate code in column readers
public interface ColumnReader
{
    Block readBlock(Type type, int batchSize)
            throws IOException;

    static ColumnReader createColumnReader(Type type, ColumnValueSelector valueSelector)
    {
        if (type == VARCHAR) {
            return new StringColumnReader(valueSelector);
        }
        if (type == DOUBLE) {
            return new DoubleColumnReader(valueSelector);
        }
        if (type == BIGINT) {
            return new LongColumnReader(valueSelector);
        }
        if (type == REAL) {
            return new FloatColumnReader(valueSelector);
        }
        if (type == TIMESTAMP) {
            return new TimestampColumnReader(valueSelector);
        }
        throw new PrestoException(DRUID_UNSUPPORTED_TYPE_ERROR, format("Unsupported type: %s", type));
    }
}
