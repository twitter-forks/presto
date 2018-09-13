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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class Assignments
{
    public static Builder builder()
    {
        return new Builder();
    }

    public static Assignments identity(Symbol... symbols)
    {
        return identity(asList(symbols));
    }

    public static Assignments identity(Iterable<Symbol> symbols)
    {
        return builder().putIdentities(symbols).build();
    }

    public static Assignments copyOf(Map<Symbol, Expression> assignments)
    {
        return builder()
                .putAll(assignments)
                .build();
    }

    public static Assignments of()
    {
        return builder().build();
    }

    public static Assignments of(Symbol symbol, Expression expression)
    {
        return builder().put(symbol, expression).build();
    }

    public static Assignments of(Symbol symbol1, Expression expression1, Symbol symbol2, Expression expression2)
    {
        return builder().put(symbol1, expression1).put(symbol2, expression2).build();
    }

    private final Map<Symbol, Expression> assignments;

    @JsonCreator
    public Assignments(@JsonProperty("assignments") Map<Symbol, Expression> assignments)
    {
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<Symbol> getOutputs()
    {
        return ImmutableList.copyOf(assignments.keySet());
    }

    @JsonProperty("assignments")
    public Map<Symbol, Expression> getMap()
    {
        return assignments;
    }

    public <C> Assignments rewrite(ExpressionRewriter<C> rewriter)
    {
        return rewrite(expression -> ExpressionTreeRewriter.rewriteWith(rewriter, expression));
    }

    public Assignments rewrite(Function<Expression, Expression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), rewrite.apply(entry.getValue())))
                .collect(toAssignments());
    }

    public Assignments rewriteEntry(Function<Entry<Symbol, Expression>, Entry<Symbol, Expression>> rewrite)
    {
        return assignments.entrySet().stream()
                .map(rewrite::apply)
                .collect(toAssignments());
    }

    public Assignments filter(Collection<Symbol> symbols)
    {
        return filter(symbols::contains);
    }

    public Assignments filter(Predicate<Symbol> predicate)
    {
        return assignments.entrySet().stream()
                .filter(entry -> predicate.test(entry.getKey()))
                .collect(toAssignments());
    }

    public boolean isIdentity(Symbol output)
    {
        Expression expression = assignments.get(output);

        return expression instanceof SymbolReference && ((SymbolReference) expression).getName().equals(output.getName());
    }

    private Collector<Entry<Symbol, Expression>, Builder, Assignments> toAssignments()
    {
        return Collector.of(
                Assignments::builder,
                (builder, entry) -> builder.put(entry.getKey(), entry.getValue()),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                Builder::build);
    }

    public Collection<Expression> getExpressions()
    {
        return assignments.values();
    }

    public Set<Symbol> getSymbols()
    {
        return assignments.keySet();
    }

    public Set<Entry<Symbol, Expression>> entrySet()
    {
        return assignments.entrySet();
    }

    public Expression get(Symbol symbol)
    {
        return assignments.get(symbol);
    }

    public int size()
    {
        return assignments.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Assignments that = (Assignments) o;

        return assignments.equals(that.assignments);
    }

    @Override
    public int hashCode()
    {
        return assignments.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("assignments", assignments)
                .toString();
    }

    public static class Builder
    {
        private final Map<Symbol, Expression> assignments = new LinkedHashMap<>();

        public Builder putAll(Assignments assignments)
        {
            return putAll(assignments.getMap());
        }

        public Builder putAll(Map<Symbol, Expression> assignments)
        {
            for (Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                put(assignment.getKey(), assignment.getValue());
            }
            return this;
        }

        public Builder put(Symbol symbol, Expression expression)
        {
            if (assignments.containsKey(symbol)) {
                Expression assignment = assignments.get(symbol);
                checkState(
                        assignment.equals(expression),
                        "Symbol %s already has assignment %s, while adding %s",
                        symbol,
                        assignment,
                        expression);
            }
            assignments.put(symbol, expression);
            return this;
        }

        public Builder put(Entry<Symbol, Expression> assignment)
        {
            put(assignment.getKey(), assignment.getValue());
            return this;
        }

        public Builder putIdentities(Iterable<Symbol> symbols)
        {
            for (Symbol symbol : symbols) {
                putIdentity(symbol);
            }
            return this;
        }

        public Builder putIdentity(Symbol symbol)
        {
            put(symbol, symbol.toSymbolReference());
            return this;
        }

        public Assignments build()
        {
            return new Assignments(assignments);
        }
    }
}
