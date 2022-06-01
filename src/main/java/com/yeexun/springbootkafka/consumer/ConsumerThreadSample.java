package com.yeexun.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/6/1 17:19
 */
public class ConsumerThreadSample {

    private final static String TOPIC_NAME = "jiangfan-topic";


    /**
     * 这种类型是经典模式, 每个线程单独创建一个KafkaConsumer, 用于保证线程安全
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerRunner r1 = new KafkaConsumerRunner();
        Thread t1 = new Thread(r1);

        t1.start();

        Thread.sleep(15000);

        r1.shutdown();
    }

    public static class  KafkaConsumerRunner implements Runnable {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer;

        public KafkaConsumerRunner() {
            Properties props = new Properties();

            props.setProperty("bootstrap.servers", "localhost:9092");
            props.setProperty("group.id", "test");
            props.setProperty("enable.auto.commit", "false");
            props.setProperty("auto.commit.interval.ms", "1000");
            props.setProperty("session.timeout.ms", "30000");
            props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<>(props);

            TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
            TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);
            TopicPartition p2 = new TopicPartition(TOPIC_NAME, 2);

            consumer.assign(Arrays.asList(p0, p1));
        }

        @Override
        public void run() {
            try {
                while (!closed.get()) {
                    //处理消息
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> pRecord = records.records(partition);

                        //处理每个分区的消息
                        for (ConsumerRecord<String, String> record : pRecord) {
                            System.out.printf("patition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());

                        }

                        //返回告诉kafka新的offset
                        long lastOffset = pRecord.get(pRecord.size() - 1).offset();

                        //注意要加1
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                    }
                }
            } catch (WakeupException e) {
                if (!closed.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }
    }

}
