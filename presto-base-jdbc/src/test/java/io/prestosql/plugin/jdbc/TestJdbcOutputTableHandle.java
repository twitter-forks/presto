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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static io.prestosql.plugin.jdbc.MetadataUtil.OUTPUT_TABLE_CODEC;
import static io.prestosql.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestJdbcOutputTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        JdbcOutputTableHandle handle = new JdbcOutputTableHandle(
                "catalog",
                "schema",
                "table",
                ImmutableList.of("abc", "xyz"),
                ImmutableList.of(VARCHAR, VARCHAR),
                "tmp_table");

        assertJsonRoundTrip(OUTPUT_TABLE_CODEC, handle);
    }
}