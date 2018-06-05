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
package com.facebook.presto.twitter.functions;

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static java.lang.String.format;

public class TwitterStringFunctions
{
    private TwitterStringFunctions()
    {
    }

    @ScalarFunction("split_every")
    @Description("Splits the string on every character and returns an array")
    @LiteralParameters({"x"})
    @SqlType("array(varchar(x))")
    public static Block str2array(@SqlType("varchar(x)") Slice utf8)
    {
        return str2array(utf8, 1, utf8.length() + 1);
    }

    @ScalarFunction("split_every")
    @Description("Splits the string on every given length of characters and returns an array")
    @LiteralParameters({"x"})
    @SqlType("array(varchar(x))")
    public static Block str2array(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long length)
    {
        return str2array(utf8, length, utf8.length() / length + 1);
    }

    @ScalarFunction("split_every")
    @Description("Splits the string on every given length of characters and returns an array with the size at most of the given limit")
    @LiteralParameters({"x"})
    @SqlType("array(varchar(x))")
    public static Block str2array(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long length, @SqlType(StandardTypes.BIGINT) long limit)
    {
        checkCondition(limit > 0, INVALID_FUNCTION_ARGUMENT, "Limit must be positive");
        checkCondition(limit <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "Limit is too large");
        checkCondition(length > 0, INVALID_FUNCTION_ARGUMENT, "Length must be positive");
        checkCondition(length <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "Length is too large");
        BlockBuilder parts = VARCHAR.createBlockBuilder(null, 1, Ints.saturatedCast(length));
        // If limit is one, the last and only element is the complete string
        if (limit == 1) {
            VARCHAR.writeSlice(parts, utf8);
            return parts.build();
        }

        int index = offsetOfCodePoint(utf8, 0);
        while (index < utf8.length()) {
            int splitIndex = offsetOfCodePoint(utf8, index, Ints.saturatedCast(length));
            // Enough remaining string?
            if (splitIndex < 0) {
                break;
            }
            // Add the part from current index to found split
            VARCHAR.writeSlice(parts, utf8, index, splitIndex - index);
            // Continue after current end
            index = splitIndex;
            // Reached limit-1 parts so we can stop
            if (parts.getPositionCount() == limit - 1) {
                break;
            }
        }
        // Rest of string
        VARCHAR.writeSlice(parts, utf8, index, utf8.length() - index);

        return parts.build();
    }

    private static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(errorCode, format(formatString, args));
        }
    }
}
