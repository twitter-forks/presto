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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

@Test
public class TestHiveThriftFieldIdResolver
{
    private static final Map<String, Short> STRUCT_FIELD_ID_AS_MAP = ImmutableMap.of(
            "0", (short) 1,
            "1", (short) 2,
            "id", (short) 4);

    private static final Map<String, Object> LIST_FIELD_ID_AS_MAP = ImmutableMap.of(
            "0", STRUCT_FIELD_ID_AS_MAP,
            "id", (short) 5);

    private static final Map<String, Short> VERBOSE_PRIMARY_FIELD_ID_AS_MAP = ImmutableMap.of(
            "id", (short) 6);

    private static final Map<String, Object> THRIFT_FIELD_ID_JSON_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("0", (short) 1)
            .put("1", (short) 3)
            .put("2", STRUCT_FIELD_ID_AS_MAP)
            .put("3", LIST_FIELD_ID_AS_MAP)
            .put("4", VERBOSE_PRIMARY_FIELD_ID_AS_MAP)
            .build();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ThriftFieldIdResolverFactory resolverFactory = new HiveThriftFieldIdResolverFactory();

    @Test
    public void testDefaultResolver()
            throws Exception
    {
        ThriftFieldIdResolver defaultResolver = resolverFactory.createResolver(new Properties());

        for (int i = 0; i <= 5; ++i) {
            assertEquals(defaultResolver.getThriftId(i), i + 1);
            assertEquals(defaultResolver.getNestedResolver(i), defaultResolver);
        }
        for (int i = 5; i >= 0; --i) {
            assertEquals(defaultResolver.getThriftId(i), i + 1);
            assertEquals(defaultResolver.getNestedResolver(i), defaultResolver);
        }
    }

    @Test
    public void testOptimizedResolver()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(THRIFT_FIELD_ID_JSON_AS_MAP);
        Properties schema = new Properties();
        schema.setProperty(HiveThriftFieldIdResolverFactory.THRIFT_FIELD_ID_JSON, json);
        ThriftFieldIdResolver resolver = resolverFactory.createResolver(schema);

        // primary field
        assertEquals(resolver.getThriftId(0), THRIFT_FIELD_ID_JSON_AS_MAP.get("0"));
        // discrete field
        assertEquals(resolver.getThriftId(1), THRIFT_FIELD_ID_JSON_AS_MAP.get("1"));

        // nested field
        ThriftFieldIdResolver nestedResolver = resolver.getNestedResolver(2);
        Map<String, Object> field = (Map<String, Object>) THRIFT_FIELD_ID_JSON_AS_MAP.get("2");
        assertEquals(resolver.getThriftId(2), field.get("id"));
        assertEquals(nestedResolver.getThriftId(0), field.get("0"));
        assertEquals(nestedResolver.getThriftId(1), field.get("1"));

        // non-nested non-primary field
        nestedResolver = resolver.getNestedResolver(3);
        field = (Map<String, Object>) THRIFT_FIELD_ID_JSON_AS_MAP.get("3");
        assertEquals(resolver.getThriftId(3), field.get("id"));

        // non-primary nested field
        nestedResolver = resolver.getNestedResolver(3);
        field = (Map<String, Object>) THRIFT_FIELD_ID_JSON_AS_MAP.get("3");
        nestedResolver = nestedResolver.getNestedResolver(0);
        field = (Map<String, Object>) field.get("0");
        assertEquals(nestedResolver.getThriftId(0), field.get("0"));
        assertEquals(nestedResolver.getThriftId(1), field.get("1"));

        // verbose primary field
        field = (Map<String, Object>) THRIFT_FIELD_ID_JSON_AS_MAP.get("4");
        assertEquals(resolver.getThriftId(4), field.get("id"));

        // non-existing field
        assertThrows(PrestoException.class, () -> resolver.getThriftId(5));
        assertThrows(PrestoException.class, () -> resolver.getNestedResolver(5));
    }
}
