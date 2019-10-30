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

import com.facebook.presto.spi.NodeManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Properties;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Kafka nodes. A worker may connect to multiple Kafka nodes depending on the segments and partitions
 * it needs to process.
 */
public class KafkaConsumerManager
{
    private static final Logger log = Logger.get(KafkaConsumerManager.class);

    public final LoadingCache<KafkaPartitionHostAddress, KafkaConsumer> consumerCache;

    private final String connectorId;
    private final NodeManager nodeManager;
    private final int connectTimeoutMillis;
    private final int maxPartitionFetchBytes;
    private final int maxPollRecords;

    @Inject
    public KafkaConsumerManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.connectTimeoutMillis = toIntExact(kafkaConnectorConfig.getKafkaConnectTimeout().toMillis());
        this.maxPartitionFetchBytes = kafkaConnectorConfig.getMaxPartitionFetchBytes();

        this.consumerCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::createConsumer));
        this.maxPollRecords = kafkaConnectorConfig.getMaxPollRecords();
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<KafkaPartitionHostAddress, KafkaConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
                consumerCache.invalidate(entry.getKey());
            }
            catch (Exception e) {
                log.warn(e, "While closing consumer %s:", entry.getKey());
            }
        }
    }

    public KafkaConsumer getConsumer(KafkaPartitionHostAddress consumerId)
    {
        requireNonNull(consumerId, "host is null");
        return consumerCache.getUnchecked(consumerId);
    }

    private KafkaConsumer createConsumer(KafkaPartitionHostAddress consumerId)
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                consumerId.hostAddress.toString());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "PRESTO_KAFKA_CONSUMERS");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteBufferDeserializer.class.getName());
        props.put("max.poll.records", Integer.toString(maxPollRecords));
        props.put("max.partition.fetch.bytes", maxPartitionFetchBytes);
        String clientId = String.join("-",
                connectorId,
                nodeManager.getCurrentNode().getHostAndPort().getHostText(),
                consumerId.toString());
        props.put("client.id", clientId);

        Thread.currentThread().setContextClassLoader(null);
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);

        return consumer;
    }
}
