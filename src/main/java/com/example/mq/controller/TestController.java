package com.example.mq.controller;

import com.example.mq.enums.MQTypeEnum;
import com.example.mq.factory.MQFactory;
import com.example.mq.model.TestEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    private final MQFactory mqFactory;

    public TestController(MQFactory mqFactory) {
        this.mqFactory = mqFactory;
    }

    @GetMapping("/send")
    public String sendMessage(
            @RequestParam("topic") String topic,
            @RequestParam("tag") String tag,
            @RequestParam("message") String message
    ) {
        // 创建测试消息事件
        TestEvent testEvent = new TestEvent(message);
        
        // 同步发送消息
        mqFactory.getProducer(MQTypeEnum.ROCKET_MQ).syncSend(MQTypeEnum.ROCKET_MQ, topic, tag, testEvent);
        
        return "消息发送成功";
    }

    @GetMapping("/send/async")
    public String sendMessageAsync(
            @RequestParam("topic") String topic,
            @RequestParam("tag") String tag,
            @RequestParam("message") String message
    ) {
        // 创建测试消息事件
        TestEvent testEvent = new TestEvent(message);
        
        // 异步发送消息
        mqFactory.getProducer(MQTypeEnum.ROCKET_MQ).asyncSend(MQTypeEnum.ROCKET_MQ, topic, tag, testEvent);
        
        return "异步消息发送成功";
    }


}