package com.yeexun.springbootkafka.multiConsumer;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/6/6 15:45
 */

import cn.hutool.json.JSONUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * 消费者组2
 */
public class Group2Listener {

    @KafkaListener(topics = {"test-topic"}, groupId = "group2")
    public void task1(ConsumerRecord record) {

        System.out.println("这是listener2, 监听的topic是: test-topic, record的值是: " + record.value());

    }
//
//    @KafkaListener(topics = {"test-topic"})
//    public void task(ConsumerRecord record) {
//
//        System.out.println("这是group2 topic test-topic的消费者 ---------->>>>>>>>:" + JSONUtil.toJsonStr(record));
//
//        Object message = record.value();
//
//        System.out.println("这是group2 topic test-topic " + record.topic());
//
//        System.out.println(message);
//
//        System.out.println(record.key());
//
//        System.out.println(record);
//
//    }
//
//    @KafkaListener(topics = {"task1"}, groupId = "group2")
//    public void task1(ConsumerRecord record) {
//
//        System.out.println("这是group2" + " task1 的消费者");
//
//        System.out.println("这是group2 topic task1 KafkaConsumer ---------->>>>>>>>:" + JSONUtil.toJsonStr(record));
//
//        Object message = record.value();
//
//        System.out.println("group2 topic task1 " + record.topic());
//
//        System.out.println(message);
//
//        System.out.println(record.key());
//
//        System.out.println(record);
//
//    }
//
//    @KafkaListener(topics = {"gift"}, groupId = "group2")
//    public void gift(ConsumerRecord<String, String> record) {
//
//        String key = record.key();
//
//        String value = record.value();
//
//        System.out.println("groupId2 kafka gift Consumer value:" + value);
//
//    }

}
