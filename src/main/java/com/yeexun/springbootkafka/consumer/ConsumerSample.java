package com.yeexun.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/5/31 17:22
 */
public class ConsumerSample {

    private final static String TOPIC_NAME = "new-topic";

    public static void main(String[] args) {
//        committedOffset();
        //手动对每个partition进行提交
        committedOffsetWithPartition();

        //手动订阅某些分区, 并提交offset
//        committedOffsetWithPartition2();
    }

    /**
     * 工作里这种用法有, 但是不推荐
     */
    private static void helloWorld() {
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
                System.out.printf("patition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }


    /**
     * 手动提交offset
     */
    private static void committedOffset() {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        //消费订阅哪一个topic或者几个topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, String> record : records) {
                //想把数据保存到数据库,成功就成功, 不成功...
                //TODO record to db

                System.out.printf("patition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());

                //如果失败则回滚, 不要提交offset

            }

            //如果成功, 则手动通知offset提交
            consumer.commitAsync();
        }
    }

    /**
     * 手动提交offset, 并且手动控制Partition
     */
    private static void committedOffsetWithPartition() {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test3");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        //消费订阅哪一个topic或者几个topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));

            //每一个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);

                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("patition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }

                long lastOffset = pRecord.get(pRecord.size() - 1).offset();

                //单个partition中的offset,并进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));

                //提交offset
                consumer.commitAsync();

                System.out.println("---------------------partition - " + partition + "处理完了 ------------------");
            }

        }
    }

    /**
     * 手动提交offset, 并且手动控制Partition, 更高级
     */
    private static void committedOffsetWithPartition2() {
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        //jiangfan-topic有两个partition
        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

//        //消费订阅哪一个topic或者几个topic
//        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 消费订阅某个topic的某个分区
        consumer.assign(Arrays.asList(p0));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));

            //每一个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);

                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("patition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }

                long lastOffset = pRecord.get(pRecord.size() - 1).offset();

                //单个partition中的offset,并进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));

                //提交offset
                consumer.commitAsync();

                System.out.println("---------------------partition - " + partition + "处理完了 ------------------");
            }

        }
    }
}
