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

import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.Optional;

public class GroupField
        extends Field
{
    private final List<Optional<Field>> children;

    public GroupField(Type type, int repetitionLevel, int definitionLevel, boolean required, List<Optional<Field>> children)
    {
        super(type, repetitionLevel, definitionLevel, required);
        this.children = children;
    }

    public List<Optional<Field>> getChildren()
    {
        return children;
    }
}
