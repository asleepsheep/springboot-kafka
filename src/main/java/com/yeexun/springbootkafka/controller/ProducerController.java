package com.yeexun.springbootkafka.controller;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Description:
 * @Author: JiangFan
 * @CreateTime 2022/5/27 16:18
 */
@RequestMapping("/kafka")
@RestController
public class ProducerController {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @GetMapping("/test")
    public String data(String msg) {
        //通过kafka发出去
        kafka.send("test", msg);
        return "ok";
    }
}
