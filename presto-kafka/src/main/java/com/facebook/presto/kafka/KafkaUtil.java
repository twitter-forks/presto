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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.SimpleConsumer;
import kafka.utils.ZKConfig;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A collection of utility methods for accessing Kafka.
 *
 * @author Raghu Angadi
 */
public final class KafkaUtil
{
    public static final ConsumerConfig DEFAULT_CONSUMER_CONFIG;

    static {
        Properties properties = new Properties();
        properties.setProperty("groupid", "this-should-not-be-used");
        DEFAULT_CONSUMER_CONFIG = new ConsumerConfig(properties);
    }

    private KafkaUtil()
    {
    }

    /**
     * create ZkClient with default options.
     */
    public static ZkClient newZkClient(String zkConnect)
    {
        // get defaults from ZkConfig.
        ZKConfig config = new ZKConfig(new Properties());

        return new ZkClient(zkConnect,
                config.zkSessionTimeoutMs(),
                config.zkConnectionTimeoutMs(),
                kafka.utils.ZKStringSerializer$.MODULE$);
    }

    /**
     * Returns partitions for given topic. An empty list if the topic is not
     * found.
     */
    public static List<Partition> getPartitionsForTopic(ZkClient zkClient,
            String topic)
    {
        // handle scala <-> java conversions.
        scala.collection.Iterator<String> topics =
                JavaConversions.asScalaIterator(Iterators.forArray(topic));
        Map<String, scala.collection.immutable.List<String>> map =
                JavaConversions.mapAsJavaMap(ZkUtils.getPartitionsForTopics(zkClient, topics));

        // since we are asking for just one topic, map's size is 0 or 1.
        if (map.size() > 0) {
            List<String> partitions = JavaConversions.seqAsJavaList(
                    map.values().iterator().next());
            // transform string to Partition object
            return Lists.newArrayList(
                    Lists.transform(partitions,
                            input -> Partition.parse(input)));
        }

        return new ArrayList<>();
    }

    /**
     * Returns latest offset before the given timestamp. If there is no offset
     * avaliable, returns the earliest available. An offset before the timestamp
     * may not be available if the messages are already rotated.
     */
    public static long getBeforeOrEarliestOffset(SimpleConsumer consumer,
            String topic,
            int partId,
            long time)
    {
        long[] offsets = consumer.getOffsetsBefore(topic, partId, time, 1);
        if (offsets.length == 0) {
            // then the earliest offset
            offsets = consumer.getOffsetsBefore(topic, partId, -2, 1);
        }

        return (offsets.length > 0) ? offsets[0] : 0;
    }

    /**
     * Returns the topics on given Kafka Server
     */
    public static List<String> getTopics(ZkClient zkClient)
    {
        String topicPath = ZkUtils.BrokerTopicsPath();
        return zkClient.getChildren(topicPath);
    }

    /**
     * Returns the brokers currently registered
     */
    public static List<Long> getBrokersIds(ZkClient zkClient)
    {
        String brokerPath = ZkUtils.BrokerIdsPath();
        List<String> brokers = zkClient.getChildren(brokerPath);
        List<Long> brokerIds = new ArrayList<Long>();
        for (String s : brokers) {
            Long l = Long.parseLong(s);
            brokerIds.add(l);
        }

        return brokerIds;
    }

    /**
     * Returns the number of partitions for a given topic and broker.
     */
    public static Integer getNumPartitions(ZkClient zkClient, String topic, Long broker)
    {
        String topicPath = ZkUtils.BrokerTopicsPath();
        String partitionPath = topicPath + "/" + topic + "/" + broker.toString();

        String numPartitions = zkClient.readData(partitionPath, true);
        if (numPartitions == null) {
            return 0;
        }
        else {
            return Integer.parseInt(numPartitions);
        }
    }

    public static SimpleConsumer newSimpleConsumer(Broker broker)
    {
        return new SimpleConsumer(broker.host(), broker.port(),
                DEFAULT_CONSUMER_CONFIG.socketTimeoutMs(),
                DEFAULT_CONSUMER_CONFIG.socketBufferSize());
    }
}
