package com.example.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.example.mq.delay.DelayMessageSender;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * RabbitMQ生产者实现
 */
@Slf4j
@Component
@ConditionalOnBean(RabbitTemplate.class)
public class RabbitMQProducer implements MQProducer {

    private final RabbitTemplate rabbitTemplate;
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;

    public RabbitMQProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }

        try {
            String messageId = "rabbitmq-" + System.currentTimeMillis();
            Message message = new Message(JSON.toJSONString(event).getBytes());
            MessageProperties properties = message.getMessageProperties();
            properties.setMessageId(messageId);
            if (tag != null) {
                properties.setHeader("tag", tag);
            }
            
            rabbitTemplate.send(topic, message);
            log.info("RabbitMQ同步发送消息成功: topic={}, tag={}, messageId={}", topic, tag, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("RabbitMQ同步发送消息失败: topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("RabbitMQ发送消息失败", e);
        }
    }

    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        CompletableFuture.runAsync(() -> {
            try {
                Message message;
                if (event instanceof String) {
                    message = new Message(((String) event).getBytes());
                } else {
                    message = new Message(JSON.toJSONString(event).getBytes());
                }
                
                MessageProperties properties = message.getMessageProperties();
                if (tag != null) {
                    properties.setHeader("tag", tag);
                }
                
                rabbitTemplate.send(topic, message);
                log.info("RabbitMQ异步发送消息成功: topic={}, tag={}", topic, tag);
            } catch (Exception e) {
                log.error("RabbitMQ异步发送消息失败: topic={}, tag={}", topic, tag, e);
            }
        });
    }

    @Override
    public String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            return null;
        }
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(topic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }



    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.RABBIT_MQ;
    }
}