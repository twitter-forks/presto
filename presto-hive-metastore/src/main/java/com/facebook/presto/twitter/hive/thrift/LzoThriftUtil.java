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
package com.facebook.presto.twitter.hive.thrift;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import static com.hadoop.compression.lzo.LzoIndex.LZO_INDEX_SUFFIX;

public class LzoThriftUtil
{
    private static final PathFilter LZOP_DEFAULT_SUFFIX_FILTER = new PathFilter()
    {
        @Override
        public boolean accept(Path path)
        {
            return path.toString().endsWith(".lzo");
        }
    };
    private static final PathFilter LZOP_INDEX_DEFAULT_SUFFIX_FILTER = new PathFilter()
    {
        @Override
        public boolean accept(Path path)
        {
            return path.toString().endsWith(".lzo.index");
        }
    };

    private LzoThriftUtil()
    {
    }

    public static Path getLzopIndexPath(Path lzoPath)
    {
        return lzoPath.suffix(LZO_INDEX_SUFFIX);
    }

    public static boolean isLzopCompressedFile(Path filePath)
    {
        return LZOP_DEFAULT_SUFFIX_FILTER.accept(filePath);
    }
}
