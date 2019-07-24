package io.prestosql.plugin.druid.column;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.apache.druid.segment.ColumnValueSelector;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.util.Objects.requireNonNull;

public class TimestampColumnReader
        implements ColumnReader
{
    private final ColumnValueSelector<Long> valueSelector;

    public TimestampColumnReader(ColumnValueSelector valueSelector)
    {
        this.valueSelector = requireNonNull(valueSelector, "value selector is null");
    }

    @Override
    public Block readBlock(Type type, int batchSize)
            throws IOException
    {
        checkArgument(type == TIMESTAMP);
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; ++i) {
            type.writeLong(builder, valueSelector.getLong());
        }

        return builder.build();
    }
}
