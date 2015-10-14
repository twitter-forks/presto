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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardManagerDao;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.metadata.ViewResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.DBIException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import static com.facebook.presto.raptor.RaptorColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.raptor.RaptorColumnHandle.SHARD_UUID_COLUMN_NAME;
import static com.facebook.presto.raptor.RaptorColumnHandle.shardRowIdHandle;
import static com.facebook.presto.raptor.RaptorColumnHandle.shardUuidColumnHandle;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorSessionProperties.getExternalBatchId;
import static com.facebook.presto.raptor.RaptorTableProperties.getSortColumns;
import static com.facebook.presto.raptor.RaptorTableProperties.getTemporalColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.metadata.MetadataDaoUtils.createMetadataTablesWithRetry;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.raptor.util.DatabaseUtil.runTransaction;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class RaptorMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(RaptorMetadata.class);

    private final IDBI dbi;
    private final MetadataDao dao;
    private final ShardManager shardManager;
    private final JsonCodec<ShardInfo> shardInfoCodec;
    private final JsonCodec<ShardDelta> shardDeltaCodec;
    private final String connectorId;

    @Inject
    public RaptorMetadata(
            RaptorConnectorId connectorId,
            @ForMetadata IDBI dbi,
            ShardManager shardManager,
            JsonCodec<ShardInfo> shardInfoCodec,
            JsonCodec<ShardDelta> shardDeltaCodec)
    {
        requireNonNull(connectorId, "connectorId is null");

        this.connectorId = connectorId.toString();
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.shardInfoCodec = requireNonNull(shardInfoCodec, "shardInfoCodec is null");
        this.shardDeltaCodec = requireNonNull(shardDeltaCodec, "shardDeltaCodec is null");

        createMetadataTablesWithRetry(dbi);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return dao.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(tableName);
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        List<TableColumn> tableColumns = dao.getTableColumns(table.getTableId());
        checkArgument(!tableColumns.isEmpty(), "Table %s does not have any columns", tableName);

        RaptorColumnHandle countColumnHandle = null;
        RaptorColumnHandle sampleWeightColumnHandle = null;
        for (TableColumn tableColumn : tableColumns) {
            if (SAMPLE_WEIGHT_COLUMN_NAME.equals(tableColumn.getColumnName())) {
                sampleWeightColumnHandle = getRaptorColumnHandle(tableColumn);
            }
            if (countColumnHandle == null && tableColumn.getDataType().getJavaType().isPrimitive()) {
                countColumnHandle = getRaptorColumnHandle(tableColumn);
            }
        }

        return new RaptorTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getTableId(),
                Optional.ofNullable(sampleWeightColumnHandle));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        List<ColumnMetadata> columns = dao.getTableColumns(handle.getTableId()).stream()
                .map(TableColumn::toColumnMetadata)
                .filter(isSampleWeightColumn().negate())
                .collect(toCollection(ArrayList::new));
        if (columns.isEmpty()) {
            throw new PrestoException(RAPTOR_ERROR, "Table does not have any columns: " + tableName);
        }

        columns.add(hiddenColumn(SHARD_UUID_COLUMN_NAME, VARCHAR));
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        return dao.listTables(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorTableHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(raptorTableHandle.getTableId())) {
            if (tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            builder.put(tableColumn.getColumnName(), getRaptorColumnHandle(tableColumn));
        }
        RaptorColumnHandle uuidColumn = shardUuidColumnHandle(connectorId);
        builder.put(uuidColumn.getColumnName(), uuidColumn);
        return builder.build();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getSampleWeightColumnHandle().orElse(null);
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();
        RaptorColumnHandle column = checkType(columnHandle, RaptorColumnHandle.class, "columnHandle");

        if (column.isShardRowId() || column.isShardUuid()) {
            return hiddenColumn(column.getColumnName(), column.getColumnType());
        }

        long columnId = column.getColumnId();
        TableColumn tableColumn = dao.getTableColumn(tableId, columnId);
        if (tableColumn == null) {
            throw new PrestoException(NOT_FOUND, format("Column ID %s does not exist for table ID %s", columnId, tableId));
        }
        return tableColumn.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(prefix.getSchemaName(), prefix.getTableName())) {
            if (tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType(), false);
            columns.put(tableColumn.getTable(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        commitCreateTable(session, beginCreateTable(session, tableMetadata), ImmutableList.of());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        long tableId = raptorHandle.getTableId();
        runTransaction(dbi, (handle, status) -> {
            ShardManagerDao shardManagerDao = handle.attach(ShardManagerDao.class);
            shardManagerDao.dropShardNodes(tableId);
            shardManagerDao.dropShards(tableId);

            MetadataDao dao = handle.attach(MetadataDao.class);
            dao.dropColumns(tableId);
            dao.dropTable(tableId);
            return null;
        });

        // TODO: add a cleanup process for leftover index tables
        // It is not possible to drop the index tables in a transaction.
        try (Handle handle = dbi.open()) {
            handle.execute("DROP TABLE " + shardIndexTable(tableId));
        }
        catch (DBIException e) {
            log.warn(e, "Failed to drop index table %s", shardIndexTable(tableId));
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        runTransaction(dbi, (handle, status) -> {
            MetadataDao dao = handle.attach(MetadataDao.class);
            dao.renameTable(table.getTableId(), newTableName.getSchemaName(), newTableName.getTableName());
            return null;
        });
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        RaptorColumnHandle sourceColumn = checkType(source, RaptorColumnHandle.class, "columnHandle");
        dao.renameColumn(table.getTableId(), sourceColumn.getColumnId(), target);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        long columnId = 1;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnHandles.add(new RaptorColumnHandle(connectorId, column.getName(), columnId, column.getType()));
            columnTypes.add(column.getType());
            columnId++;
        }
        Map<String, RaptorColumnHandle> columnHandleMap = Maps.uniqueIndex(columnHandles.build(), RaptorColumnHandle::getColumnName);

        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(getSortColumns(tableMetadata.getProperties()), columnHandleMap);
        Optional<RaptorColumnHandle> temporalColumnHandle = getTemporalColumnHandle(getTemporalColumn(tableMetadata.getProperties()), columnHandleMap);

        RaptorColumnHandle sampleWeightColumnHandle = null;
        if (tableMetadata.isSampled()) {
            sampleWeightColumnHandle = new RaptorColumnHandle(connectorId, SAMPLE_WEIGHT_COLUMN_NAME, columnId, BIGINT);
            columnHandles.add(sampleWeightColumnHandle);
            columnTypes.add(BIGINT);
        }

        return new RaptorOutputTableHandle(
                connectorId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnHandles.build(),
                columnTypes.build(),
                Optional.ofNullable(sampleWeightColumnHandle),
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST),
                temporalColumnHandle);
    }

    private static Optional<RaptorColumnHandle> getTemporalColumnHandle(String temporalColumn, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        if (temporalColumn == null) {
            return Optional.empty();
        }

        RaptorColumnHandle handle = columnHandleMap.get(temporalColumn);
        if (handle == null) {
            throw new PrestoException(NOT_FOUND, format("Temporal column %s does not exist", temporalColumn));
        }
        return Optional.of(handle);
    }

    private static List<RaptorColumnHandle> getSortColumnHandles(List<String> sortColumns, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        if (sortColumns == null) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<RaptorColumnHandle> sortColumnHandles = ImmutableList.builder();
        for (String column : sortColumns) {
            RaptorColumnHandle handle = columnHandleMap.get(column);
            if (handle == null) {
                throw new PrestoException(NOT_FOUND, format("Ordering column %s does not exist", column));
            }
            sortColumnHandles.add(handle);
        }
        return sortColumnHandles.build();
    }

    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, Collection<Slice> fragments)
    {
        RaptorOutputTableHandle table = checkType(outputTableHandle, RaptorOutputTableHandle.class, "outputTableHandle");

        if (table.getTemporalColumnHandle().isPresent()) {
            RaptorColumnHandle column = table.getTemporalColumnHandle().get();
            if (!column.getColumnType().equals(TIMESTAMP) && !column.getColumnType().equals(DATE)) {
                throw new PrestoException(NOT_SUPPORTED, "Temporal column must be of type timestamp or date: " + column.getColumnName());
            }
        }

        long newTableId = runTransaction(dbi, (dbiHandle, status) -> {
            MetadataDao dao = dbiHandle.attach(MetadataDao.class);
            long tableId = dao.insertTable(table.getSchemaName(), table.getTableName(), true);
            List<RaptorColumnHandle> sortColumnHandles = table.getSortColumnHandles();

            for (int i = 0; i < table.getColumnTypes().size(); i++) {
                RaptorColumnHandle column = table.getColumnHandles().get(i);

                int columnId = i + 1;
                Integer sortPosition = !sortColumnHandles.contains(column) ? null : sortColumnHandles.indexOf(column);
                dao.insertColumn(tableId, columnId, column.getColumnName(), i, table.getColumnTypes().get(i).getTypeSignature().toString(), sortPosition);

                if (table.getTemporalColumnHandle().isPresent() && table.getTemporalColumnHandle().get().equals(column)) {
                    dao.updateTemporalColumnId(tableId, columnId);
                }
            }

            return tableId;
        });

        List<ColumnInfo> columns = table.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());

        // TODO: refactor this to avoid creating an empty table on failure
        shardManager.createTable(newTableId, columns);
        shardManager.commitShards(newTableId, columns, parseFragments(fragments), Optional.empty());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();

        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (TableColumn column : dao.getTableColumns(tableId)) {
            columnHandles.add(new RaptorColumnHandle(connectorId, column.getColumnName(), column.getColumnId(), column.getDataType()));
            columnTypes.add(column.getDataType());
        }

        Optional<String> externalBatchId = getExternalBatchId(session);
        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(tableId);
        return new RaptorInsertTableHandle(connectorId,
                tableId,
                columnHandles.build(),
                columnTypes.build(),
                externalBatchId,
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST));
    }

    private List<RaptorColumnHandle> getSortColumnHandles(long tableId)
    {
        ImmutableList.Builder<RaptorColumnHandle> builder = ImmutableList.builder();
        for (TableColumn tableColumn : dao.listSortColumns(tableId)) {
            checkArgument(!tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME), "sample weight column may not be a sort column");
            builder.add(getRaptorColumnHandle(tableColumn));
        }
        return builder.build();
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        RaptorInsertTableHandle handle = checkType(insertHandle, RaptorInsertTableHandle.class, "insertHandle");
        long tableId = handle.getTableId();
        Optional<String> externalBatchId = handle.getExternalBatchId();
        List<ColumnInfo> columns = handle.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());

        shardManager.commitShards(tableId, columns, parseFragments(fragments), externalBatchId);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return shardRowIdHandle(connectorId);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void commitDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();

        List<ColumnInfo> columns = getColumnHandles(session, tableHandle).values().stream()
                .map(handle -> checkType(handle, RaptorColumnHandle.class, "columnHandle"))
                .map(ColumnInfo::fromHandle).collect(toList());

        ImmutableSet.Builder<UUID> oldShardUuids = ImmutableSet.builder();
        ImmutableList.Builder<ShardInfo> newShards = ImmutableList.builder();

        fragments.stream()
                .map(fragment -> shardDeltaCodec.fromJson(fragment.getBytes()))
                .forEach(delta -> {
                    oldShardUuids.addAll(delta.getOldShardUuids());
                    newShards.addAll(delta.getNewShards());
                });

        shardManager.replaceShardUuids(tableId, columns, oldShardUuids.build(), newShards.build());
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        String schemaName = viewName.getSchemaName();
        String tableName = viewName.getTableName();

        if (replace) {
            runTransaction(dbi, (handle, status) -> {
                MetadataDao dao = handle.attach(MetadataDao.class);
                dao.dropView(schemaName, tableName);
                dao.insertView(schemaName, tableName, viewData);
                return null;
            });
            return;
        }

        try {
            dao.insertView(schemaName, tableName, viewData);
        }
        catch (PrestoException e) {
            if (viewExists(session, viewName)) {
                throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
            }
            throw e;
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (!viewExists(session, viewName)) {
            throw new ViewNotFoundException(viewName);
        }
        dao.dropView(viewName.getSchemaName(), viewName.getTableName());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return dao.listViews(schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> map = ImmutableMap.builder();
        for (ViewResult view : dao.getViews(prefix.getSchemaName(), prefix.getTableName())) {
            map.put(view.getName(), new ConnectorViewDefinition(view.getName(), Optional.empty(), view.getData()));
        }
        return map.build();
    }

    private boolean viewExists(ConnectorSession session, SchemaTableName viewName)
    {
        return !getViews(session, viewName.toSchemaTablePrefix()).isEmpty();
    }

    private RaptorColumnHandle getRaptorColumnHandle(TableColumn tableColumn)
    {
        return new RaptorColumnHandle(connectorId, tableColumn.getColumnName(), tableColumn.getColumnId(), tableColumn.getDataType());
    }

    private Collection<ShardInfo> parseFragments(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(fragment -> shardInfoCodec.fromJson(fragment.getBytes()))
                .collect(toList());
    }

    private static Predicate<ColumnMetadata> isSampleWeightColumn()
    {
        return input -> input.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME);
    }

    private static ColumnMetadata hiddenColumn(String name, Type type)
    {
        return new ColumnMetadata(name, type, false, null, true);
    }
}
