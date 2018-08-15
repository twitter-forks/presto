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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.FieldSet;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PickColumnLayouts
        implements Rule<TableScanNode>
{
    private final Metadata metadata;
    private static final Pattern<TableScanNode> PATTERN = tableScan();

    public PickColumnLayouts(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode node, Captures captures, Context context)
    {
        System.err.println("##############PickColumnLayouts:apply#############");
        System.err.println(node.getAssignments());

        List<Map.Entry<ColumnHandle, FieldSet>> fieldSets = node.getAssignments().keySet().stream()
                .filter(symbol -> !symbol.getFields().isEmpty())
                .map(symbol -> Maps.immutableEntry(node.getAssignments().get(symbol), new FieldSet(symbol.getFields())))
                .collect(toImmutableList());

        ImmutableMap.Builder<ColumnHandle, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < fieldSets.size(); i++) {
            builder.put(fieldSets.get(i).getKey(), i);
        }
        Map<ColumnHandle, Integer> columnIndex = builder.build();

        if (fieldSets.isEmpty()) {
            return Result.empty();
        }

        List<TableLayoutResult> layouts = metadata.getLayouts(
                context.getSession(),
                node.getTable(),
                new Constraint<>(Optional.of(fieldSets)),
                Optional.of(ImmutableSet.copyOf(node.getAssignments().values())));

        TableLayoutResult layout = layouts.get(0);

        if (!layout.getLayout().getColumns().isPresent() || layout.getLayout().getColumns().get().isEmpty()) {
            return Result.empty();
        }

        List<ColumnHandle> columns = layout.getLayout().getColumns().get();

        Map<Symbol, ColumnHandle> assignments = node.getAssignments().keySet().stream()
                .filter(symbol -> !symbol.getFields().isEmpty())
                .map(symbol -> Maps.immutableEntry(symbol, columns.get(columnIndex.get(node.getAssignments().get(symbol)))))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return Result.ofPlanNode(new TableScanNode(
                node.getId(),
                node.getTable(),
                node.getOutputSymbols(),
                node.getAssignments().keySet().stream().map(symbol -> Maps.immutableEntry(symbol, assignments.getOrDefault(symbol, node.getAssignments().get(symbol)))).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
                node.getLayout(),
                node.getCurrentConstraint(),
                node.getOriginalConstraint()));
    }
}
