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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HiveCluster;
import com.facebook.presto.hive.HiveViewNotSupportedException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.ThriftHiveMetastoreClient;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.isPrestoView;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.metastore.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilege.parsePrivilege;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS;

public class UserBasedHiveMetastore
        implements HiveMetastore
{
    private final UserBasedHiveMetastoreStats stats = new UserBasedHiveMetastoreStats();
    protected final HiveCluster clientProvider;

    @Inject
    public UserBasedHiveMetastore(HiveCluster hiveCluster)
    {
        this.clientProvider = requireNonNull(hiveCluster, "hiveCluster is null");
    }

    @Managed
    @Flatten
    public UserBasedHiveMetastoreStats getStats()
    {
        return stats;
    }

    protected Function<Exception, Exception> getExceptionMapper()
    {
        return Function.identity();
    }

    @Override
    public void createTable(String user, Table table)
    {
        try {
            retry()
                    .exceptionMapper(getExceptionMapper())
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            client.createTable(table);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(table.getDbName());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropTable(String user, String databaseName, String tableName)
    {
        try {
            retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            client.dropTable(databaseName, tableName, true);
                        }
                        return null;
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw com.google.common.base.Throwables.propagate(e);
        }
    }

    @Override
    public void alterTable(String user, String databaseName, String tableName, Table table)
    {
        try {
            retry()
                    .exceptionMapper(getExceptionMapper())
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTable", stats.getAlterTable().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            Optional<Table> source = getTable(user, databaseName, tableName);
                            if (!source.isPresent()) {
                                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                            }
                            client.getClient().set_ugi(user, ImmutableList.of());
                            client.alterTable(databaseName, tableName, table);
                        }
                        return null;
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (InvalidOperationException | MetaException e) {
            throw com.google.common.base.Throwables.propagate(e);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw com.google.common.base.Throwables.propagate(e);
        }
    }

    @Override
    public void flushCache()
    {
    }

    @Override
    public List<String> getAllDatabases(String user)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            return client.getAllDatabases();
                        }
                    }));
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(String user, String databaseName)
    {
        Callable<List<String>> getAllTables = stats.getGetAllTables().wrap(() -> {
            try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                client.getClient().set_ugi(user, ImmutableList.of());
                return client.getAllTables(databaseName);
            }
        });

        Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
            try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                client.getClient().set_ugi(user, ImmutableList.of());
                client.getDatabase(databaseName);
                return null;
            }
        });

        try {
            return retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", () -> {
                        List<String> tables = getAllTables.call();
                        if (tables.isEmpty()) {
                            // Check to see if the database exists
                            getDatabase.call();
                        }
                        return Optional.of(tables);
                    });
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(String user, String databaseName)
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getAllViews().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            String filter = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
                            client.getClient().set_ugi(user, ImmutableList.of());
                            return Optional.of(client.getTableNamesByFilter(databaseName, filter));
                        }
                    }));
        }
        catch (UnknownDBException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String user, String databaseName)
    {
        try {
            return retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            return Optional.of(client.getDatabase(databaseName));
                        }
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(String user, String databaseName, String tableName, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            retry()
                    .exceptionMapper(getExceptionMapper())
                    .stopOn(org.apache.hadoop.hive.metastore.api.AlreadyExistsException.class, org.apache.hadoop.hive.metastore.api.InvalidObjectException.class, MetaException.class, org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class, PrestoException.class)
                    .stopOnIllegalExceptions()
                    .run("addPartitions", stats.getAddPartitions().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            int partitionsAdded = client.addPartitions(partitions);
                            if (partitionsAdded != partitions.size()) {
                                throw new PrestoException(HIVE_METASTORE_ERROR,
                                        format("Hive metastore only added %s of %s partitions", partitionsAdded, partitions.size()));
                            }
                        }
                        return null;
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException | org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            // todo partition already exists exception
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw com.google.common.base.Throwables.propagate(e);
        }
    }

    @Override
    public void dropPartition(String user, String databaseName, String tableName, List<String> parts)
    {
        try {
            retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartition", stats.getDropPartition().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            client.dropPartition(databaseName, tableName, parts, true);
                        }
                        return null;
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw com.google.common.base.Throwables.propagate(e);
        }
    }

    @Override
    public void dropPartitionByName(String user, String databaseName, String tableName, String partitionName)
    {
        try {
            retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartitionByName", stats.getDropPartitionByName().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            // It is observed that: (examples below assumes a table with one partition column `ds`)
                            //  * When a partition doesn't exist (e.g. ds=2015-09-99), this thrift call is a no-op. It doesn't throw any exception.
                            //  * When a typo exists in partition column name (e.g. dxs=2015-09-01), this thrift call will delete ds=2015-09-01.
                            client.getClient().set_ugi(user, ImmutableList.of());
                            client.dropPartitionByName(databaseName, tableName, partitionName, true);
                        }
                        return null;
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw com.google.common.base.Throwables.propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNames(String user, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNames", stats.getGetPartitionNames().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            return Optional.of(client.getPartitionNames(databaseName, tableName));
                        }
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String user, String databaseName, String tableName, List<String> parts)
    {
        try {
            return retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesPs().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            return Optional.of(client.getPartitionNamesFiltered(databaseName, tableName, parts));
                        }
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Partition> getPartition(String user, String databaseName, String tableName, String partitionName)
    {
        requireNonNull(partitionName, "partitionName is null");
        try {
            return retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionByName().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            return Optional.of(client.getPartitionByName(databaseName, tableName, partitionName));
                        }
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Map<String, Partition>> getPartitionsByNames(String user, String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        try {
            return retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            List<String> partitionColumnNames = ImmutableList.copyOf(Warehouse.makeSpecFromName(partitionNames.get(0)).keySet());
                            ImmutableMap.Builder<String, Partition> partitions = ImmutableMap.builder();
                            client.getClient().set_ugi(user, ImmutableList.of());
                            for (Partition partition : client.getPartitionsByNames(databaseName, tableName, partitionNames)) {
                                String partitionId = FileUtils.makePartName(partitionColumnNames, partition.getValues(), null);
                                partitions.put(partitionId, partition);
                            }
                            return Optional.of(partitions.build());
                        }
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            // assume none of the partitions in the batch are available
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(String user, String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(org.apache.hadoop.hive.metastore.api.NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() -> {
                        try (ThriftHiveMetastoreClient client = (ThriftHiveMetastoreClient) clientProvider.createMetastoreClient()) {
                            client.getClient().set_ugi(user, ImmutableList.of());
                            Table table = client.getTable(databaseName, tableName);
                            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) && (!isPrestoView(table))) {
                                throw new HiveViewNotSupportedException(new SchemaTableName(databaseName, tableName));
                            }
                            return Optional.of(table);
                        }
                    }));
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<String> getRoles(String user)
    {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            List<Role> roles = client.listRoles(user, USER);
            if (roles == null) {
                return ImmutableSet.of();
            }
            return ImmutableSet.copyOf(roles.stream()
                    .map(Role::getRoleName)
                    .collect(toSet()));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<HivePrivilege> getDatabasePrivileges(String user, String databaseName)
    {
        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();

        if (isDatabaseOwner(user, databaseName)) {
            privileges.add(OWNERSHIP);
        }
        privileges.addAll(getPrivileges(user, new HiveObjectRef(HiveObjectType.DATABASE, databaseName, null, null, null)));

        return privileges.build();
    }

    @Override
    public Set<HivePrivilege> getTablePrivileges(String user, String databaseName, String tableName)
    {
        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();

        if (isTableOwner(user, databaseName, tableName)) {
            privileges.add(OWNERSHIP);
        }
        privileges.addAll(getPrivileges(user, new HiveObjectRef(HiveObjectType.TABLE, databaseName, tableName, null, null)));

        return privileges.build();
    }

    private Set<HivePrivilege> getPrivileges(String user, HiveObjectRef objectReference)
    {
        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            PrincipalPrivilegeSet privilegeSet = client.getPrivilegeSet(objectReference, user, null);

            if (privilegeSet != null) {
                Map<String, List<PrivilegeGrantInfo>> userPrivileges = privilegeSet.getUserPrivileges();
                if (userPrivileges != null) {
                    privileges.addAll(toGrants(userPrivileges.get(user)));
                }
                for (List<PrivilegeGrantInfo> rolePrivileges : privilegeSet.getRolePrivileges().values()) {
                    privileges.addAll(toGrants(rolePrivileges));
                }
                // We do not add the group permissions as Hive does not seem to process these
            }
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        return privileges.build();
    }

    private static Set<HivePrivilege> toGrants(List<PrivilegeGrantInfo> userGrants)
    {
        if (userGrants == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<HivePrivilege> privileges = ImmutableSet.builder();
        for (PrivilegeGrantInfo userGrant : userGrants) {
            privileges.addAll(parsePrivilege(userGrant));
            if (userGrant.isGrantOption()) {
                privileges.add(HivePrivilege.GRANT);
            }
        }
        return privileges.build();
    }
}
