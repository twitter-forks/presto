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
package com.facebook.presto.kafka;

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertColumnHandle;
import static com.facebook.presto.kafka.KafkaHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Kafka connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link KafkaInternalFieldDescription} for a list
 * of per-topic additional columns.
 */
public class KafkaMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(KafkaMetadata.class);

    private final String connectorId;
    private final boolean hideInternalColumns;
    private final Map<SchemaTableName, KafkaTopicDescription> tableDescriptions;
    private final Set<KafkaInternalFieldDescription> internalFieldDescriptions;

    @Inject
    public KafkaMetadata(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            Supplier<Map<SchemaTableName, KafkaTopicDescription>> kafkaTableDescriptionSupplier,
            Set<KafkaInternalFieldDescription> internalFieldDescriptions)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.hideInternalColumns = kafkaConnectorConfig.isHideInternalColumns();

        requireNonNull(kafkaTableDescriptionSupplier, "kafkaTableDescriptionSupplier is null");
        this.tableDescriptions = kafkaTableDescriptionSupplier.get();
        this.internalFieldDescriptions = requireNonNull(internalFieldDescriptions, "internalFieldDescriptions is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            return null;
        }

        return new KafkaTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getTopicName(),
                getDataFormat(table.getKey()),
                getDataFormat(table.getMessage()));
    }

    private static String getDataFormat(KafkaTopicFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = convertTableHandle(tableHandle);

        KafkaTopicDescription kafkaTopicDescription = tableDescriptions.get(kafkaTableHandle.toSchemaTableName());
        if (kafkaTopicDescription == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
        KafkaTopicFieldGroup key = kafkaTopicDescription.getKey();
        if (key != null) {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, true, index++));
                }
            }
        }

        KafkaTopicFieldGroup message = kafkaTopicDescription.getMessage();
        if (message != null) {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, false, index++));
                }
            }
        }

        for (KafkaInternalFieldDescription kafkaInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(kafkaInternalFieldDescription.getName(), kafkaInternalFieldDescription.getColumnHandle(connectorId, index++, hideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        KafkaTableHandle handle = convertTableHandle(table);

        Long startTs = null;
        Long endTs = null;
        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        log.info(constraint.getSummary().toString(null));
        if (domains.isPresent()) {
            Map<ColumnHandle, Domain> columnHandleDomainMap = domains.get();
            for (Map.Entry<ColumnHandle, Domain> entry : columnHandleDomainMap.entrySet()) {
                if (entry.getKey() instanceof KafkaColumnHandle && ((KafkaColumnHandle) entry.getKey()).getName().equals(KafkaInternalFieldDescription.OFFSET_TIMESTAMP_FIELD.getName())) {
                    Range span = entry.getValue().getValues().getRanges().getSpan();
                    Marker low = span.getLow();
                    Marker high = span.getHigh();
                    if (!low.isLowerUnbounded()) {
                        startTs = (Long) low.getValue();
                    }
                    if (!high.isUpperUnbounded()) {
                        endTs = (Long) high.getValue();
                    }
                }

                log.info("K: %s\tV: %s", entry.getKey().toString(), entry.getValue().toString());
            }
        }

        log.info("startTs: %s, endTs: %s", startTs, endTs);
        ConnectorTableLayout layout = new ConnectorTableLayout(new KafkaTableLayoutHandle(handle, startTs, endTs));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        KafkaTopicFieldGroup key = table.getKey();
        if (key != null) {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        KafkaTopicFieldGroup message = table.getMessage();
        if (message != null) {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        for (KafkaInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
