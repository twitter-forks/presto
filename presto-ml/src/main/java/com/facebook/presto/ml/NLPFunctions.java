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
package com.facebook.presto.ml;

import com.facebook.presto.operator.scalar.WordStemFunction;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class NLPFunctions
{
    private NLPFunctions()
    {
    }

    @ScalarFunction("remove_punc")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice removePunc(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return Slices.utf8Slice(slice.toStringUtf8().replaceAll("[^a-zA-Z ]", " "));
    }

    @ScalarFunction("stem")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stem(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        String[] words = slice.toStringUtf8().split(" ");
        String stemmed = Arrays
                .stream(words)
                .map(word -> WordStemFunction.wordStem(Slices.utf8Slice(word)).toStringUtf8())
                .collect(Collectors.joining(" "));
        return Slices.utf8Slice(stemmed);
    }
}
