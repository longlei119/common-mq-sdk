package com.example.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.example.mq.delay.DelayMessageSender;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * ActiveMQ生产者实现
 */
@Slf4j
@Component
@ConditionalOnBean(JmsTemplate.class)
public class ActiveMQProducer implements MQProducer {

    private final JmsTemplate jmsTemplate;
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;

    public ActiveMQProducer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        try {
            String messageId = "activemq-" + System.currentTimeMillis();
            jmsTemplate.convertAndSend(topic, JSON.toJSONString(event));
            log.info("ActiveMQ同步发送消息成功: topic={}, tag={}, messageId={}", topic, tag, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("ActiveMQ同步发送消息失败: topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("ActiveMQ发送消息失败", e);
        }
    }

    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        CompletableFuture.runAsync(() -> {
            try {
                jmsTemplate.convertAndSend(topic, event);
                log.info("ActiveMQ异步发送消息成功: topic={}, tag={}", topic, tag);
            } catch (Exception e) {
                log.error("ActiveMQ异步发送消息失败: topic={}, tag={}", topic, tag, e);
            }
        });
    }

    @Override
    public String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            return null;
        }
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(topic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }



    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.ACTIVE_MQ;
    }

    private String buildDestination(String topic, String tag) {
        if (tag != null && !tag.isEmpty()) {
            return topic + "." + tag;
        }
        return topic;
    }
}