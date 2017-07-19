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
package com.facebook.presto.kafka.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import scala.Option;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.kafka.util.TestUtils.findUnusedPort;
import static com.facebook.presto.kafka.util.TestUtils.toProperties;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Objects.requireNonNull;

public class EmbeddedKafka
        implements Closeable
{
    private final EmbeddedZookeeper zookeeper;
    private final int port;
    private final File kafkaDataDir;
    private final KafkaServerStartable kafka;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public static EmbeddedKafka createEmbeddedKafka()
            throws IOException
    {
        return new EmbeddedKafka(new EmbeddedZookeeper(), new Properties());
    }

    public static EmbeddedKafka createEmbeddedKafka(Properties overrideProperties)
            throws IOException
    {
        return new EmbeddedKafka(new EmbeddedZookeeper(), overrideProperties);
    }

    EmbeddedKafka(EmbeddedZookeeper zookeeper, Properties overrideProperties)
            throws IOException
    {
        this.zookeeper = requireNonNull(zookeeper, "zookeeper is null");
        requireNonNull(overrideProperties, "overrideProperties is null");

        this.port = findUnusedPort();
        this.kafkaDataDir = Files.createTempDir();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("brokerid", "0")
                .put("regionid", "1")
                .put("hostname", "localhost")
                .put("num.partitions", "2")
                .put("log.flush.interval", "10000")
                .put("log.default.flush.interval.ms", "1000")
                .put("log.retention.hours", "1")
                .put("log.segment.bytes", "10248576")
                .put("log.file.size", "1048576")
                .put("auto.create.topics.enable", "false")
                .put("zookeeper.connection.timeout.ms", "1000000")
                .put("port", Integer.toString(port))
                .put("log.dir", kafkaDataDir.getAbsolutePath())
                .put("zookeeper.connect", zookeeper.getConnectString())
                .put("zk.connect", zookeeper.getConnectString())
                .putAll(Maps.fromProperties(overrideProperties))
                .build();

        KafkaConfig config = new KafkaConfig(toProperties(properties));
        this.kafka = new KafkaServerStartable(config, Option.empty());
    }

    public void start()
            throws InterruptedException, IOException
    {
        if (!started.getAndSet(true)) {
            zookeeper.start();
            kafka.startup();
        }
    }

    @Override
    public void close()
    {
        if (started.get() && !stopped.getAndSet(true)) {
            kafka.shutdown();
            kafka.awaitShutdown();
            zookeeper.close();
            deleteRecursively(kafkaDataDir);
        }
    }

    public void createTopics(String... topics)
    {
        createTopics(2, 1, new Properties(), topics);
    }

    public void createTopics(int partitions, int replication, Properties topicProperties, String... topics)
    {
        // noop
    }

    public CloseableProducer<Long, Object> createProducer()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("broker.list", String.format("0:%s", getConnectString()))
                .put("serializer.class", JsonEncoder.class.getName())
                .put("key.serializer.class", NumberEncoder.class.getName())
                .put("request.required.acks", "1")
                .build();

        ProducerConfig producerConfig = new ProducerConfig(toProperties(properties));
        return new CloseableProducer<>(producerConfig);
    }

    public static class CloseableProducer<K, V>
            extends Producer<K, V>
            implements AutoCloseable
    {
        public CloseableProducer(ProducerConfig config)
        {
            super(config);
        }
    }

    public int getZookeeperPort()
    {
        return zookeeper.getPort();
    }

    public int getPort()
    {
        return port;
    }

    public String getConnectString()
    {
        return "localhost:" + Integer.toString(port);
    }

    public String getZookeeperConnectString()
    {
        return zookeeper.getConnectString();
    }
}
