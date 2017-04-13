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

import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;

public class HiveThriftFieldIdResolver
        implements ThriftFieldIdResolver
{
    private static final Logger log = Logger.get(HiveThriftFieldIdResolver.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String THRIFT_FIELD_ID_JSON = "thrift.field.id.json";
    private final JsonNode root;

    public HiveThriftFieldIdResolver()
    {
        this.root = null;
    }

    public HiveThriftFieldIdResolver(JsonNode root)
    {
        this.root = root;
    }

    public ThriftFieldIdResolver initialize(Properties schema)
    {
        String jsonData = schema.getProperty(THRIFT_FIELD_ID_JSON);
        try {
            return new HiveThriftFieldIdResolver(objectMapper.readTree(jsonData));
        }
        catch (Exception e) {
            log.debug("Got an exception %s in initialize, schema: %s", e.getMessage(), schema);
            return new HiveThriftFieldIdResolver();
        }
    }

    public ThriftFieldIdResolver getNestedResolver(int hiveIndex)
    {
        try {
            return new HiveThriftFieldIdResolver(root.get(String.valueOf(hiveIndex)));
        }
        catch (Exception e) {
            log.debug("Got an exception %s in getNestedResolver, root: %s, want the hiveIndex: %s", e.getMessage(), root, hiveIndex);
            return new HiveThriftFieldIdResolver();
        }
    }

    public short getThriftId(int hiveIndex)
    {
        try {
            return (short) root.get(String.valueOf(hiveIndex)).get("id").asInt();
        }
        catch (Exception e) {
            log.debug("Got an exception %s in getThriftId, root: %s, want the hiveIndex: %s", e.getMessage(), root, hiveIndex);
            return (short) (hiveIndex + 1);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("root", root)
                .toString();
    }
}
