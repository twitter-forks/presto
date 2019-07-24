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
package io.prestosql.plugin.druid;

import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.util.Objects.requireNonNull;

public class DruidSplitManager
        implements ConnectorSplitManager
{
    private final DruidClient druidClient;

    @Inject
    public DruidSplitManager(DruidClient druidClient)
    {
        this.druidClient = requireNonNull(druidClient, "druid client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        DruidTableHandle table = (DruidTableHandle) tableHandle;
        List<String> segmentIds = getSegmentId(table);

        // TODO: add scheduling
        List<DruidSplit> splits = segmentIds.stream()
                .map(id -> druidClient.getSingleSegmentInfo(table.getTableName(), id))
                .map(info -> new DruidSplit(info, HostAddress.fromUri(druidClient.getDruidBroker())))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }

    public List<String> getSegmentId(DruidTableHandle table)
    {
        TupleDomain<ColumnHandle> constraint = table.getConstraint();

        if (constraint.getDomains().isPresent()) {
            Domain domain = constraint.getDomains().get().get(new DruidColumnHandle("__time", TIMESTAMP));
            if (domain != null) {
                return druidClient.getDataSegmentIdInDomain(table.getTableName(), domain);
            }
        }
        return druidClient.getAllDataSegmentId(table.getTableName());
    }
}
