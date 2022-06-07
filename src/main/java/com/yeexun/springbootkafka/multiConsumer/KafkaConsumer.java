package com.yeexun.springbootkafka.multiConsumer;

import cn.hutool.json.JSONUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/6/6 14:35
 */

//通过注解监听topic进行消费
@Configuration
@EnableKafka
public class KafkaConsumer {

    private Map consumerConfigs() {

        Map props = new HashMap();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        System.out.println("KafkaConsumer consumerConfigs " + JSONUtil.toJsonStr(props));

        return props;

    }

    /**
     * 获取工厂
     */
    private ConsumerFactory consumerFactory() {

        return new DefaultKafkaConsumerFactory(consumerConfigs());
    }

    /**
     * 获取实例
     */
    @Bean
    public KafkaListenerContainerFactory kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory factory1 = new ConcurrentKafkaListenerContainerFactory<>();

        factory1.setConsumerFactory(consumerFactory());
        factory1.setConcurrency(2);
        factory1.getContainerProperties().setPollTimeout(3000);

        System.out.println("KafkaConsumer kafkaListenerContainerFactory factory" + JSONUtil.toJsonStr(factory1));

        return factory1;

    }

    /**
     * topic的消费者组1监听
     *
     * @return
     */
    @Bean
    public Group1Listener listener1() {

        return new Group1Listener();
    }

}

