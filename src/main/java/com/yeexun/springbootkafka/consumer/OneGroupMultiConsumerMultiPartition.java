package com.yeexun.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/6/2 10:35
 */
public class OneGroupMultiConsumerMultiPartition {

    private final static String TOPIC_NAME = "jiangfan-topic";
    private final static String GROUP_NAME = "test";

    public static void main(String[] args) {

//        consume();
        getPartitionsForTopic();
    }

    @KafkaListener(containerGroup=GROUP_NAME,topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME,partitions = {"0"}),

    })
    public static void consume() {

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        //消费订阅哪一个topic或者几个topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, String> record : records) {

                System.out.println("topic名称: " + record.topic() + ", 第" + record.partition() +"个partition: " + ", 值是多少:" + record.value() + ", 当前的offset是:" + record.offset());

            }

        }
    }



    public static void getPartitionsForTopic() {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        Collection<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC_NAME);
        System.out.println("获取partition的信息------------:");
        List<org.apache.kafka.common.TopicPartition> tp =new ArrayList<>();
        partitionInfos.forEach(str -> {
            System.out.println("Partition的信息为:");
            System.out.println(str);

            tp.add(new org.apache.kafka.common.TopicPartition(TOPIC_NAME, str.partition()));
            consumer.assign(tp);
            consumer.seekToEnd(tp);

            System.out.println("Partition " + str.partition() + " 的最后offet为: '" + consumer.position(new org.apache.kafka.common.TopicPartition(TOPIC_NAME, str.partition())));
        });
    }

}
