package com.yeexun.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.annotation.KafkaListener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/6/2 10:35
 */
public class OneGroupMultiConsumerMultiPartition {

    private final static String TOPIC_NAME = "new-topic";
    private final static String GROUP_NAME = "test";

    public static void main(String[] args) {

//        consume();
//        getPartitionsForTopic();
        textResetOffset();
    }

    @KafkaListener(containerGroup = GROUP_NAME, topicPartitions = {
            @org.springframework.kafka.annotation.TopicPartition(topic = TOPIC_NAME, partitions = {"0"}),
    })
    public static void consume() {

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test3");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        //消费订阅哪一个topic或者几个topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, String> record : records) {

                System.out.println("topic名称: " + record.topic() + ", 第" + record.partition() + "个partition: " + ", 值是多少:" + record.value() + ", 当前的offset是:" + record.offset());

            }

        }
    }


    public static void getPartitionsForTopic() {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test3");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        Collection<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC_NAME);
        System.out.println("获取partition的信息------------:");
        List<org.apache.kafka.common.TopicPartition> tp = new ArrayList<>();
        partitionInfos.forEach(str -> {
//            System.out.println("Partition的信息为:");
//            System.out.println(str);

            tp.add(new org.apache.kafka.common.TopicPartition(TOPIC_NAME, str.partition()));
            consumer.assign(tp);
            consumer.seekToEnd(tp);

            System.out.println("Partition " + str.partition() + " 的最后offet为: '" + consumer.position(new org.apache.kafka.common.TopicPartition(TOPIC_NAME, str.partition())));
        });
    }


    private static void textResetOffset() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe incoming topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        //get consumer consume partitions
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC_NAME);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        }
        // poll data from kafka server to prevent lazy operation
        consumer.poll(Duration.ofSeconds(1));

        //reset offset from beginning
        consumer.seekToBeginning(topicPartitions);

        //reset designated partition offset by designated spot
        int offset = 20;
        consumer.seek(topicPartitions.get(0), offset);

        //reset offset to end
        consumer.seekToEnd(topicPartitions);

        //consume message as usual
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<String, String> record = iterator.next();
            System.out.println("consume data: " + record.value());
        }
    }

}
