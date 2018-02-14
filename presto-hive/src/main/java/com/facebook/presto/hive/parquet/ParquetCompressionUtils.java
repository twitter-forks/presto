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

import com.hadoop.compression.lzo.LzoCodec;
import io.airlift.compress.Decompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import parquet.bytes.BytesInput;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class ParquetCompressionUtils
{
    private static final int GZIP_BUFFER_SIZE = 8 * 1024;

    private ParquetCompressionUtils() {}

    public static Slice decompress(CompressionCodecName codec, Slice input, int uncompressedSize)
            throws IOException
    {
        requireNonNull(input, "input is null");

        if (input.length() == 0) {
            return EMPTY_SLICE;
        }

        switch (codec) {
            case GZIP:
                return decompressGzip(input, uncompressedSize);
            case SNAPPY:
                return decompressSnappy(input, uncompressedSize);
            case UNCOMPRESSED:
                return input;
            case LZO:
                return decompressLZO(input, uncompressedSize);
            default:
                throw new ParquetCorruptionException("Codec not supported in Parquet: " + codec);
        }
    }

    private static Slice decompressSnappy(Slice input, int uncompressedSize)
    {
        byte[] buffer = new byte[uncompressedSize];
        decompress(new SnappyDecompressor(), input, 0, input.length(), buffer, 0);
        return wrappedBuffer(buffer);
    }

    private static Slice decompressGzip(Slice input, int uncompressedSize)
            throws IOException
    {
        if (uncompressedSize == 0) {
            return EMPTY_SLICE;
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(uncompressedSize);
        byte[] buffer = new byte[uncompressedSize];
        try (InputStream gzipInputStream = new GZIPInputStream(input.getInput(), GZIP_BUFFER_SIZE)) {
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                sliceOutput.write(buffer, 0, bytesRead);
            }
            return sliceOutput.getUnderlyingSlice();
        }
    }

    // decompress LZO-compressed data using HADOOP-LZO codec
    private static Slice decompressLZO(Slice input, int uncompressedSize)
            throws IOException
    {
        final BytesInput decompressed;
        LzoCodec codec = new LzoCodec();
        codec.setConf(new Configuration());
        org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();

        // input data
        byte[] byteArray = (byte[]) input.getBase();
        int inputOffset = (int)input.getAddress() - ARRAY_BYTE_BASE_OFFSET;
        ByteArrayBytesInput bytes = new ByteArrayBytesInput(byteArray, inputOffset, input.length());

        decompressor.reset();
        InputStream is = codec.createInputStream(toInputStream(bytes), decompressor);
        decompressed = BytesInput.from(is, uncompressedSize);
        checkArgument(decompressed.size() == uncompressedSize);
        return wrappedBuffer(decompressed.toByteArray(), 0, uncompressedSize);
    }

    private static int decompress(Decompressor decompressor, Slice input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    {
        byte[] byteArray = (byte[]) input.getBase();
        int byteArrayOffset = inputOffset + (int) (input.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int size = decompressor.decompress(byteArray, byteArrayOffset, inputLength, output, outputOffset, output.length - outputOffset);
        return size;
    }

    // helper class and helper functions from org.apache.parquet.bytes
    private static class ByteArrayBytesInput
            extends BytesInput
    {
        private final byte[] in;
        private final int offset;
        private final int length;

        private ByteArrayBytesInput(byte[] in, int offset, int length)
        {
            this.in = in;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public void writeAllTo(OutputStream out)
                throws IOException
        {
            out.write(in, offset, length);
        }

        @Override
        public long size()
        {
            return length;
        }
    }

    private static final class BAOS
            extends ByteArrayOutputStream
    {
        private BAOS(int size)
        {
            super(size);
        }

        public byte[] getBuf()
        {
            return this.buf;
        }
    }

    private static byte[] toByteArray(BytesInput bytes)
            throws IOException
    {
        BAOS baos = new BAOS((int) bytes.size());
        bytes.writeAllTo(baos);
        return baos.getBuf();
    }

    private static InputStream toInputStream(BytesInput bytes)
            throws IOException
    {
        return new ByteBufferInputStream(toByteBuffer(bytes));
    }

    private static ByteBuffer toByteBuffer(BytesInput bytes)
            throws IOException
    {
        return ByteBuffer.wrap(toByteArray(bytes));
    }
}
