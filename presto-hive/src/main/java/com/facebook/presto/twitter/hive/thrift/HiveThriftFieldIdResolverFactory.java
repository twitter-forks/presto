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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.Properties;

public class HiveThriftFieldIdResolverFactory
        implements ThriftFieldIdResolverFactory
{
    private static final Logger log = Logger.get(HiveThriftFieldIdResolverFactory.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String THRIFT_FIELD_ID_JSON = "thrift.field.id.json";

    public ThriftFieldIdResolver createResolver(Properties schema)
    {
        JsonNode root = null;
        String jsonData = schema.getProperty(THRIFT_FIELD_ID_JSON);
        if (jsonData != null) {
            try {
                root = objectMapper.readTree(jsonData);
            }
            catch (IOException e) {
                log.debug(e, "Failed to createResolver, schema: %s", schema);
            }
        }
        return new HiveThriftFieldIdResolver(root);
    }
}
