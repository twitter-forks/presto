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
package com.facebook.presto.decoder.thrift;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.google.common.base.Splitter;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Thrift specific row decoder
 */
public class ThriftRowDecoder
        implements RowDecoder
{
    public static final String NAME = "thrift";

    private final Map<DecoderColumnHandle, ThriftColumnDecoder> columnDecoders;

    public ThriftRowDecoder(Set<DecoderColumnHandle> columnHandles)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        columnDecoders = columnHandles.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private ThriftColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new ThriftColumnDecoder(columnHandle);
    }

    private static Object locateNode(Map<Short, Object> map, DecoderColumnHandle columnHandle)
    {
        Map<Short, Object> currentLevel = map;
        Object val = null;

        Iterator<String> it = Splitter.on('/').omitEmptyStrings().split(columnHandle.getMapping()).iterator();
        while (it.hasNext()) {
            String pathElement = it.next();
            Short key = Short.valueOf(pathElement);
            val = currentLevel.get(key);

            // could be because of optional fields
            if (val == null) {
                return null;
            }

            if (val instanceof ThriftGenericRow) {
                currentLevel = ((ThriftGenericRow) val).getValues();
            }
            else if (it.hasNext()) {
                throw new IllegalStateException("Invalid thrift field schema");
            }
        }

        return val;
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        ThriftGenericRow row = new ThriftGenericRow();
        try {
            TDeserializer deser = new TDeserializer();
            deser.deserialize(row, data);
            row.parse();
        }
        catch (TException e) {
            return Optional.empty();
        }

        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decode(locateNode(row.getValues(), entry.getKey())))));
    }
}
