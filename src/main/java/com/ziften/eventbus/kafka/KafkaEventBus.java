/*
 *  Copyright 2012-2016 Vasil Kostov
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ziften.eventbus.kafka;


import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import kafka.cluster.Broker;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class KafkaEventBus implements Watcher {

    private EventBus guavaEventBus;

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventBus.class);
    private static final String DEFAULT_TOPIC = "EVENTS";

    private ZooKeeper zooKeeper;
    private String groupId;
    private Producer<String, String> producer;
    private ConsumerConnector consumer;
    private List<String> topics = Collections.singletonList(DEFAULT_TOPIC);
    private Integer consumerThreads = 1;
    private final ExecutorService executor;

    public KafkaEventBus(String zookeeperConnect, String groupId) {
        this(zookeeperConnect, groupId, Executors.newFixedThreadPool(5));
    }

    public KafkaEventBus(String zookeeperConnect, String groupId, ExecutorService executor) {
        this.guavaEventBus = new AsyncEventBus(groupId, executor);
        this.groupId = groupId;
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeperConnect, groupId));
        this.executor = executor;
        try {
            this.zooKeeper = new ZooKeeper(zookeeperConnect, 10000, this);
            this.producer = createKafkaProducer();
        } catch (IOException e) {
            logger.error("Failed to connect to zookeeper", e);
        }
    }

    public void register(Object listener) {
        guavaEventBus.register(listener);
    }

    public void unregister(Object listener) {
        guavaEventBus.unregister(listener);
    }

    public void post(Object event) {
        post(event, DEFAULT_TOPIC);
    }

    public void post(Object event, String topic) {
        KeyedMessage<String, String> msg = new KeyedMessage<>(topic, event.toString());
        producer.send(msg);
    }

    public void start() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        for (String topic : topics) {
            topicCountMap.put(topic, consumerThreads);
        }
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(DEFAULT_TOPIC);

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new MessageConsumer(stream, threadNumber++));
        }

    }

    public void stop() {
        if (consumer != null) consumer.shutdown();
        if(executor != null) executor.shutdown();

        try {
            if(!executor.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                logger.error("Timed out waiting for consumer threads to shutdown, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted during shutdown, exiting uncleanly");
        }
    }

    private void consumeKafkaMessage(byte[] message) {
        logger.debug("Consuming message from kafka topic {}", new String(message));
        guavaEventBus.post(message);
    }


    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    private Producer<String, String> createKafkaProducer() {
        try {
            String brokerList = String.join(",", getBrokerListFromZookeeper());
            return new Producer<>(createProducerConfig(brokerList));
        } catch (Exception e) {
            logger.error("Failed to create Kafka producer", e);
            return null;
        }
    }

    private ProducerConfig createProducerConfig(String brokerList) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        return new ProducerConfig(props);
    }

    private List<String> getBrokerListFromZookeeper() throws Exception {
        List<String> brokerList = new ArrayList<>();
        List<String> ids = zooKeeper.getChildren("/brokers/ids", false);
        for (String id : ids) {
            String brokerInfoString = new String(zooKeeper.getData("/brokers/ids/" + id, false, null));
            Broker broker = Broker.createBroker(Integer.valueOf(id), brokerInfoString);
            if (broker != null) {
                brokerList.add(broker.connectionString());
            }
        }
        return brokerList;
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("ZK watcher event received: {}", event);
    }

    private class MessageConsumer implements Runnable {
        private KafkaStream<byte[], byte[]> stream;
        private int threadNumber;

        public MessageConsumer(KafkaStream<byte[], byte[]> stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;
        }

        @Override
        public void run() {
            logger.info("Starting consumer thread {}", threadNumber);
            ConsumerIterator<byte[], byte[]> it = stream.iterator();

            while (it.hasNext()) {
                consumeKafkaMessage(it.next().message());
            }

            logger.info("Shutting down consumer thread {}", threadNumber);
        }
    }


    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void setProducer(Producer<String, String> producer) {
        this.producer = producer;
    }

    public void setConsumer(ConsumerConnector consumer) {
        this.consumer = consumer;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public void setConsumerThreads(Integer consumerThreads) {
        this.consumerThreads = consumerThreads;
    }
}
