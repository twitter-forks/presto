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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricAggregation;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.state.MinMaxNState;
import com.facebook.presto.operator.aggregation.state.MinMaxNStateFactory;
import com.facebook.presto.operator.aggregation.state.MinMaxNStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.metadata.FunctionType.AGGREGATE;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMinMaxNAggregation
        extends ParametricAggregation
{
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AbstractMinMaxNAggregation.class, "input", BlockComparator.class, Type.class, MinMaxNState.class, Block.class, long.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMinMaxNAggregation.class, "combine", MinMaxNState.class, MinMaxNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMinMaxNAggregation.class, "output", ArrayType.class, MinMaxNState.class, BlockBuilder.class);

    private final String name;
    private final Function<Type, BlockComparator> typeToComparator;
    private final Signature signature;

    protected AbstractMinMaxNAggregation(String name, Function<Type, BlockComparator> typeToComparator)
    {
        requireNonNull(name);
        requireNonNull(typeToComparator);
        this.name = name;
        this.typeToComparator = typeToComparator;
        this.signature = new Signature(name, AGGREGATE, ImmutableList.of(orderableTypeParameter("E")), "array<E>", ImmutableList.of("E", StandardTypes.BIGINT), false);
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        Signature signature = new Signature(name, AGGREGATE, parameterizedTypeName("array", type.getTypeSignature()), type.getTypeSignature(), BIGINT.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(type);
        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    protected InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AbstractMinMaxNAggregation.class.getClassLoader());

        BlockComparator comparator = typeToComparator.apply(type);
        List<Type> inputTypes = ImmutableList.of(type, BIGINT);
        MinMaxNStateSerializer stateSerializer = new MinMaxNStateSerializer(comparator, type);
        Type intermediateType = stateSerializer.getSerializedType();
        ArrayType outputType = new ArrayType(type);

        List<ParameterMetadata> inputParameterMetadata =  ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(INPUT_CHANNEL, BIGINT),
                new ParameterMetadata(BLOCK_INDEX));

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, type, inputTypes),
                inputParameterMetadata,
                INPUT_FUNCTION.bindTo(comparator).bindTo(type),
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(outputType),
                MinMaxNState.class,
                stateSerializer,
                new MinMaxNStateFactory(),
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, inputTypes, intermediateType, outputType, true, false, factory);
    }

    public static void input(BlockComparator comparator, Type type, MinMaxNState state, Block block, long n, int blockIndex)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            if (n <= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "second argument of max_n/min_n must be positive");
            }
            heap = new TypedHeap(comparator, type, Ints.checkedCast(n));
            state.setTypedHeap(heap);
        }
        long startSize = heap.getEstimatedSize();
        heap.add(block, blockIndex);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void combine(MinMaxNState state, MinMaxNState otherState)
    {
        TypedHeap otherHeap = otherState.getTypedHeap();
        if (otherHeap == null) {
            return;
        }
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            state.setTypedHeap(otherHeap);
            return;
        }
        long startSize = heap.getEstimatedSize();
        heap.addAll(otherHeap);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void output(ArrayType outputType, MinMaxNState state, BlockBuilder out)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null || heap.isEmpty()) {
            out.appendNull();
            return;
        }

        Type elementType = outputType.getElementType();

        BlockBuilder reversedBlockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), heap.getCapacity());
        long startSize = heap.getEstimatedSize();
        heap.popAll(reversedBlockBuilder);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);

        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        for (int i = reversedBlockBuilder.getPositionCount() - 1; i >= 0; i--) {
            elementType.appendTo(reversedBlockBuilder, i, arrayBlockBuilder);
        }
        out.closeEntry();
    }
}
