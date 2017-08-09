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
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.cluster.Partition;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaSimpleConsumerManager consumerManager;
    private final KafkaConnectorConfig config;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaSimpleConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.config = kafkaConnectorConfig;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
        ZkClient zkClient = KafkaUtil.newZkClient(config.getZkEndpoint());

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        Cluster cluster = ZkUtils.getCluster(zkClient);
        List<Partition> partitions = KafkaUtil.getPartitionsForTopic(zkClient, kafkaTableHandle.getTopicName());

        long estimatedTotalSize = 0L;

        for (Partition part : partitions) {
            log.debug("Adding Partition %s/%s from broker %s", kafkaTableHandle.getTopicName(), part.partId(), part.brokerId());
            Broker leader = cluster.getBroker(part.brokerId()).get();

            if (leader == null) { // Leader election going on...
                log.error("No leader for partition %s/%s found!", kafkaTableHandle.getTopicName(), part.partId());
                continue;
            }

            HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());

            SimpleConsumer leaderConsumer = consumerManager.getConsumer(partitionLeader);
            // Kafka contains a reverse list of "end - start" pairs for the splits

            KafkaTableLayoutHandle layoutHandle = (KafkaTableLayoutHandle) layout;
            long startTs = layoutHandle.getOffsetStartTs();
            long endTs = layoutHandle.getOffsetEndTs();

            long[] offsets = findAllOffsets(leaderConsumer,  kafkaTableHandle.getTopicName(), part.partId(), startTs, endTs);
            for (int i = offsets.length - 1; i > 0; i--) {
                KafkaSplit split = new KafkaSplit(
                        connectorId,
                        kafkaTableHandle.getTopicName(),
                        kafkaTableHandle.getKeyDataFormat(),
                        kafkaTableHandle.getMessageDataFormat(),
                        part.partId(),
                        offsets[i],
                        offsets[i - 1],
                        partitionLeader,
                        startTs,
                        endTs);
                splits.add(split);

                long splitSize = (split.getEnd() - split.getStart()) / 1024 / 1024;
                log.debug("Split summarize: %s-%s (%sMB)", split.getStart(), split.getEnd(), splitSize);
                estimatedTotalSize += splitSize;
            }
        }

        ImmutableList<ConnectorSplit> builtSplits = splits.build();
        log.info("Built " + builtSplits.size() + " splits");

        log.info("EstimatedTotalSize: %s", estimatedTotalSize);
        return new FixedSplitSource(builtSplits);
    }

    private static long[] findAllOffsets(SimpleConsumer consumer, String topicName, int partitionId, long startTs, long endTs)
    {
        // startTs: start timestamp, or -2/null as earliest
        // endTs: end timestamp, or -1/null as latest
        if (startTs >= endTs && endTs != OffsetRequest.LatestTime()) {
            throw new IllegalArgumentException(String.format("Invalid Kafka Offset start/end pair: %s - %s", startTs, endTs));
        }

        long[] offsetsBeforeStartTs = consumer.getOffsetsBefore(topicName, partitionId, startTs, Integer.MAX_VALUE);
        long[] offsetsBeforeEndTs = consumer.getOffsetsBefore(topicName, partitionId, endTs, Integer.MAX_VALUE);
        log.debug("NumOffsetsBeforeStartTs=%s, NumOffsetsBeforeEndTs=%s", offsetsBeforeStartTs.length, offsetsBeforeEndTs.length);

        if (offsetsBeforeStartTs.length == 0) {
            return offsetsBeforeEndTs;
        }

        long[] offsets = new long[offsetsBeforeEndTs.length - offsetsBeforeStartTs.length + 1];
        long startOffset = offsetsBeforeStartTs[0];

        for (int i = 0; i < offsetsBeforeEndTs.length; i++) {
            if (offsetsBeforeEndTs[i] == startOffset) {
                offsets[i] = startOffset;
                break;
            }
            offsets[i] = offsetsBeforeEndTs[i];
        }

        return offsets;
    }
}
