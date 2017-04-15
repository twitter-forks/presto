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

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static com.google.common.base.MoreObjects.toStringHelper;

public class HiveThriftFieldIdResolver
        implements ThriftFieldIdResolver
{
    private final JsonNode root;
    private final Map<Integer, ThriftFieldIdResolver> nestedResolvers = new HashMap<>();
    private final Map<Integer, Short> thriftIds = new HashMap<>();

    public HiveThriftFieldIdResolver(JsonNode root)
    {
        this.root = root;
    }

    public short getThriftId(int hiveIndex)
    {
        if (root == null) {
            return (short) (hiveIndex + 1);
        }

        Short thriftId = thriftIds.get(Integer.valueOf(hiveIndex));
        if (thriftId != null) {
            return thriftId.shortValue();
        }
        else {
            JsonNode child = root.get(String.valueOf(hiveIndex));
            checkCondition(child != null, HIVE_INVALID_METADATA, "Missed json value for hiveIndex: %s, root: %s", hiveIndex, root);
            checkCondition(child.get("id") != null, HIVE_INVALID_METADATA, "Missed key id for hiveIndex: %s, root: %s", hiveIndex, root);
            thriftId = Short.valueOf((short) child.get("id").asInt());
            thriftIds.put(Integer.valueOf(hiveIndex), thriftId);
            return thriftId;
        }
    }

    public ThriftFieldIdResolver getNestedResolver(int hiveIndex)
    {
        ThriftFieldIdResolver nestedResolver = nestedResolvers.get(Integer.valueOf(hiveIndex));
        if (nestedResolver != null) {
            return nestedResolver;
        }
        else {
            JsonNode child = null;
            if (root != null) {
                child = root.get(String.valueOf(hiveIndex));
            }
            // what if the child == null?
            // checkCondition(child != null, HIVE_INVALID_METADATA, "Missed json value for hiveIndex: %s, root: %s", hiveIndex, root);
            nestedResolver = new HiveThriftFieldIdResolver(child);
            nestedResolvers.put(Integer.valueOf(hiveIndex), nestedResolver);
            return nestedResolver;
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("root", root)
                .add("nestedResolvers", nestedResolvers)
                .toString();
    }
}
