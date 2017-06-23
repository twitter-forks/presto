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
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Set;

import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ThriftFieldDecoder
        implements FieldDecoder<Object>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return ThriftRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public FieldValueProvider decode(Object value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");
        return new ObjectValueProvider(value, columnHandle);
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }

    public static class ObjectValueProvider
            extends FieldValueProvider
    {
        protected final Object value;
        protected final DecoderColumnHandle columnHandle;

        public ObjectValueProvider(Object value, DecoderColumnHandle columnHandle)
        {
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.value = value;
        }

        @Override
        public final boolean accept(DecoderColumnHandle columnHandle)
        {
            return this.columnHandle.equals(columnHandle);
        }

        @Override
        public final boolean isNull()
        {
            return value == null;
        }

        @Override
        public boolean getBoolean()
        {
            return isNull() ? false : (Boolean) value;
        }

        @Override
        public long getLong()
        {
            return isNull() ? 0L : ((Number) value).longValue();
        }

        @Override
        public double getDouble()
        {
            return isNull() ? 0.0d : (Double) value;
        }

        @Override
        public Slice getSlice()
        {
            if (isNull()) {
                return EMPTY_SLICE;
            }

            Slice slice = utf8Slice(value.toString());
            if (isVarcharType(columnHandle.getType())) {
                slice = truncateToLength(slice, columnHandle.getType());
            }
            return slice;
        }
    }
}
