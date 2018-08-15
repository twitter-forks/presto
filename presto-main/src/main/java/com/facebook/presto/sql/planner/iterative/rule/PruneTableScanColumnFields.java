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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class PruneTableScanColumnFields
        extends ProjectOffPushDownFieldRule<TableScanNode>
{
    public PruneTableScanColumnFields()
    {
        super(tableScan());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, TableScanNode tableScanNode, Set<Symbol> referencedOutputs)
    {
        System.err.println("----PruneTableScanColumns::referencedOutputs----");
        System.err.println(referencedOutputs);
        return Optional.of(
                new TableScanNode(
                        tableScanNode.getId(),
                        tableScanNode.getTable(),
                        filteredCopy(tableScanNode.getOutputSymbols(), referencedOutputs),
                        filterKeys(tableScanNode.getAssignments(), referencedOutputs),
                        tableScanNode.getLayout(),
                        tableScanNode.getCurrentConstraint(),
                        tableScanNode.getOriginalConstraint()));
    }

    private List<Symbol> filteredCopy(List<Symbol> outputSymbols, Set<Symbol> referencedOutputs)
    {
        Map<String, Symbol> referencedOutputsMap = referencedOutputs.stream()
                .collect(toImmutableMap(Symbol::getName, symbol -> symbol));
        return outputSymbols.stream()
                .filter(symbol -> referencedOutputsMap.containsKey(symbol.getName()))
                .map(symbol -> referencedOutputsMap.get(symbol.getName()))
                .collect(toImmutableList());
    }

    private Map<Symbol, ColumnHandle> filterKeys(Map<Symbol, ColumnHandle> assignements, Set<Symbol> referencedOutputs)
    {
        Map<String, Symbol> referencedOutputsMap = referencedOutputs.stream()
                .collect(toImmutableMap(Symbol::getName, symbol -> symbol));
        return assignements.keySet().stream()
                .filter(symbol -> referencedOutputsMap.containsKey(symbol.getName()))
                .map(symbol -> Maps.immutableEntry(referencedOutputsMap.get(symbol.getName()), assignements.get(symbol)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
