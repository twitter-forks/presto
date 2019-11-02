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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import io.airlift.log.Logger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaConsumerManager consumerManager;
    private final Set<HostAddress> nodes;
    private final KafkaZookeeperServerset servers;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaZookeeperServerset servers,
            KafkaConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.nodes = ImmutableSet.copyOf(kafkaConnectorConfig.getNodes());
        this.servers = servers;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
        try {
            HostAddress node = (!this.nodes.isEmpty()) ? selectRandom(this.nodes) : servers.selectRandomServer();

            int partition = 0;
            KafkaThreadPartitionIdentifier consumerId = new KafkaThreadPartitionIdentifier(Integer.toString(partition), Thread.currentThread().getName(), node);
            String topic = kafkaTableHandle.getTopicName();
            KafkaConsumer consumer = consumerManager.getConsumer(consumerId);

            List<PartitionInfo> parts = consumer.partitionsFor(topic);

            log.debug("Build a new consumer %s for topic: %s to broker %s part: %s", consumerId.toString(), topic, node.toString(), parts.toString());
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            for (PartitionInfo part : parts) {
                Node leader = part.leader();
                if (leader == null) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Leader election in progress for Kafka topic '%s' partition %s", topic, part.partition()));
                }

                HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());
                KafkaTableLayoutHandle layoutHandle = (KafkaTableLayoutHandle) layout;
                long startTs = layoutHandle.getOffsetStartTs();
                long endTs = layoutHandle.getOffsetEndTs();

                KafkaSplit split = new KafkaSplit(
                        connectorId,
                        topic,
                        kafkaTableHandle.getKeyDataFormat(),
                        kafkaTableHandle.getMessageDataFormat(),
                        kafkaTableHandle.getKeyDataSchemaLocation().map(KafkaSplitManager::readSchema),
                        kafkaTableHandle.getMessageDataSchemaLocation().map(KafkaSplitManager::readSchema),
                        part.partition(),
                        startTs,
                        endTs,
                        partitionLeader);
                splits.add(split);
            }

            consumerManager.tearDown();
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private static String readSchema(String dataSchemaLocation)
    {
        InputStream inputStream = null;
        try {
            if (isURI(dataSchemaLocation.trim().toLowerCase(ENGLISH))) {
                try {
                    inputStream = new URL(dataSchemaLocation).openStream();
                }
                catch (MalformedURLException e) {
                    // try again before failing
                    inputStream = new FileInputStream(dataSchemaLocation);
                }
            }
            else {
                inputStream = new FileInputStream(dataSchemaLocation);
            }
            return CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Could not parse the Avro schema at: " + dataSchemaLocation, e);
        }
        finally {
            closeQuietly(inputStream);
        }
    }

    private static void closeQuietly(InputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static boolean isURI(String location)
    {
        try {
            URI.create(location);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    private static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
