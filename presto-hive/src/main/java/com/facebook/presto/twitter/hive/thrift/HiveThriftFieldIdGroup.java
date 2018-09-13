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

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static java.util.Objects.requireNonNull;

public class HiveThriftFieldIdGroup
{
    private final Map<Short, HiveThriftFieldIdGroup> fields;

    public HiveThriftFieldIdGroup(Map<Short, HiveThriftFieldIdGroup> fields)
    {
        this.fields = requireNonNull(fields, "fields is null");
    }

    public HiveThriftFieldIdGroup getFieldIdGroup(short thriftId)
    {
        return fields.get(thriftId);
    }

    public Set<Short> getFieldIds()
    {
        return ImmutableSet.copyOf(fields.keySet());
    }

    public static HiveThriftFieldIdGroup create(Type type, Set<String> requiredFields, ThriftFieldIdResolver fieldIdResolver)
    {
        if (requiredFields.isEmpty() || (!isStructuralType(type))) {
            return new HiveThriftFieldIdGroup(ImmutableMap.of());
        }

        Map<String, Set<String>> fields = groupFields(requiredFields);

        List<String> fieldNames = type.getTypeParameters().stream()
                .map(Type::getDisplayName)
                .collect(toImmutableList());

        return new HiveThriftFieldIdGroup(fields.entrySet().stream()
                .filter(entry -> fieldNames.contains(entry.getKey()))
                .map(entry -> immutableEntry(fieldNames.indexOf(entry.getKey()), entry.getValue()))
                .collect(toImmutableMap(entry -> fieldIdResolver.getThriftId(entry.getKey()),
                        entry -> create(type.getTypeParameters().get(entry.getKey()),
                                entry.getValue(), fieldIdResolver.getNestedResolver(entry.getKey())))));
    }

    private static Map<String, Set<String>> groupFields(Set<String> requiredFields)
    {
        Map<String, Set<String>> fields = new HashMap<>();
        for (String field : requiredFields) {
            String[] path = field.split("\\.", 2);
            String fieldName = path[0];
            if (fields.containsKey(fieldName) && fields.get(fieldName).isEmpty()) {
                continue;
            }
            Set<String> nestedField = path.length == 1 ? ImmutableSet.of() : ImmutableSet.of(path[1]);
            if (fields.containsKey(fieldName) && nestedField.isEmpty()) {
                fields.get(fieldName).clear();
            }
            else if (fields.containsKey(fieldName)) {
                fields.get(fieldName).addAll(nestedField);
            }
            else {
                fields.put(fieldName, new HashSet<>(nestedField));
            }
        }

        return ImmutableMap.copyOf(fields);
    }
}
