package com.yeexun.springbootkafka.multiConsumer;

import cn.hutool.json.JSONUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.Optional;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/6/6 15:44
 */
public class Group1Listener {

    @KafkaListener(topics = {"test-topic"}, groupId = "group1")
    public static void task1(ConsumerRecord record) {

        System.out.println("这是listener1, 监听的topic是: test-topic" + "record的值是: " + record.value());

//        Object message = record.value();
//
//        System.out.println("group1 topic task1 " + record.topic());
//
//        System.out.println(message);
//
//        System.out.println(record.key());
//
//        System.out.println(record);

    }

//    @KafkaListener(topics = {"test-topic"})
//    public static void listen(ConsumerRecord record) {
//
//        Optional kafkaMessage = Optional.ofNullable(record.value());
//
//        if (kafkaMessage.isPresent()) {
//
//            Object message = kafkaMessage.get();
//
//            System.out.println("listen1 " + message);
//
//        }
//
//    }
//
//    @KafkaListener(topics = {"test-topic"}, groupId = "group1")
//    public static void task1(ConsumerRecord record) {
//
//        System.out.println("这是" + " task1 的消费者");
//
//        System.out.println("这是group1 topic task1 KafkaConsumer ---------->>>>>>>>:" + JSONUtil.toJsonStr(record));
//
//        Object message = record.value();
//
//        System.out.println("group1 topic task1 " + record.topic());
//
//        System.out.println(message);
//
//        System.out.println(record.key());
//
//        System.out.println(record);
//
//    }
//
//    @KafkaListener(topics = {"gift"}, groupId = "group1")
//    public static void gift(ConsumerRecord<String, String> record) {
//
//        String key = record.key();
//
//        String value = record.value();
//
//        System.out.println("groupId1 kafka gift Consumer value:" + value);
//
//    }

}
