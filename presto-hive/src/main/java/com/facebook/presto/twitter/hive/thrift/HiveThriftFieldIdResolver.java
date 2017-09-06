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

/**
 * Resolve the translation of continuous hive ids to discontinuous thrift ids by using a json property.
 * Example:
 * We have the thrift definition:
 *
 *     struct Name {
 *         1: string first,
 *         2: string last
 *     }
 *     struct Person {
 *         1: Name name,
 *         3: String phone
 *     }
 *
 * Hive table for Person:
 *
 *     +---------+-------------+----------------------------------+-----------------+
 *     | hive id | column name | type                             | thrift field id |
 *     +---------+-------------+----------------------------------+-----------------+
 *     | 0       | name        | struct<first:string,last:string> | 1               |
 *     +---------+-------------+----------------------------------+-----------------+
 *     | 1       | phone       | string                           | 3               |
 *     +---------+-------------+----------------------------------+-----------------+
 *
 * The corresponding id mapping object is:
 *
 *     x = {
 *         '0': {
 *              '0': 1,
 *              '1': 2,
 *              'id': 1
 *         },
 *         '1': 3
 *     }
 *
 * The json property is:
 *
 *     {"0":{"0":1,"1":2,"id":1},"1":3}
 */
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

    @Override
    public short getThriftId(int hiveIndex)
    {
        if (root == null) {
            return (short) (hiveIndex + 1);
        }

        Short thriftId = thriftIds.get(hiveIndex);
        if (thriftId != null) {
            return thriftId;
        }
        else {
            JsonNode child = root.get(String.valueOf(hiveIndex));
            checkCondition(child != null, HIVE_INVALID_METADATA, "Missed json value for hiveIndex: %s, root: %s", hiveIndex, root);
            if (child.isNumber()) {
                thriftId = (short) child.asInt();
            }
            else {
                checkCondition(child.get("id") != null, HIVE_INVALID_METADATA, "Missed id for hiveIndex: %s, root: %s", hiveIndex, root);
                thriftId = (short) child.get("id").asInt();
            }
            thriftIds.put(hiveIndex, thriftId);
            return thriftId;
        }
    }

    @Override
    public ThriftFieldIdResolver getNestedResolver(int hiveIndex)
    {
        if (root == null) {
            return this;
        }

        ThriftFieldIdResolver nestedResolver = nestedResolvers.get(hiveIndex);
        if (nestedResolver != null) {
            return nestedResolver;
        }
        else {
            JsonNode child = root.get(String.valueOf(hiveIndex));
            checkCondition(child != null, HIVE_INVALID_METADATA, "Missed json value for hiveIndex: %s, root: %s", hiveIndex, root);
            nestedResolver = new HiveThriftFieldIdResolver(child);
            nestedResolvers.put(hiveIndex, nestedResolver);
            return nestedResolver;
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("root", root)
                .add("nestedResolvers", nestedResolvers)
                .add("thriftIds", thriftIds)
                .toString();
    }
}
