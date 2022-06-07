package com.yeexun.springbootkafka.multiConsumer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/6/6 14:16
 */
public class KafkaProducer {

    private static KafkaTemplate kafkaTemplate = new KafkaTemplate<>(producerFactory());

    public static void send(String topic, String key, String data){

        ListenableFuture future = kafkaTemplate.send(topic, key, data);

        future.addCallback(new CallBackSuccess(),new FailCallBack(topic, key, data));

    }

    private static void send(String topic, Integer parti, Long time, Object key, String value){

        kafkaTemplate.send(topic,parti,time,key,value);

    }


    /**
     * 获取配置
     *
     * @return
     */
    private static Map producerConfigs() {

        Map props = new HashMap();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 4096);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 40960);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;

    }

    /** 获取工厂 */
    private static ProducerFactory producerFactory() {

        return new DefaultKafkaProducerFactory(producerConfigs());

    }


    /**
     * 发送消息后的成功回调
     */
    static class CallBackSuccess implements SuccessCallback {

        @Override
        public void onSuccess(Object o) {

            System.out.println("成功");
        }
    }

    /**
     * 发送消息后的失败回调
     */
    static class FailCallBack implements FailureCallback {

        String topic;
        String key;
        String data;

        FailCallBack(String topic, String key, String data){
            this.data = data;
            this.key = key;
            this.topic = topic;
        }

        @Override
        public void onFailure(Throwable throwable) {

            System.out.println("失败 topicId:"+topic+",key:"+key+",data:"+data);
            throwable.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{

        KafkaTemplate hh = kafkaTemplate;

        System.out.print("kafkaTemplate是--" + hh);

        for (int i=0; i < 10; i++){

            ListenableFuture r = hh.send("test-topic","key2","发出的消息"+i);

            r.addCallback(new CallBackSuccess(),new FailCallBack("","",""));

            hh.flush();

            Thread.sleep(1000);

        }

    }
}
