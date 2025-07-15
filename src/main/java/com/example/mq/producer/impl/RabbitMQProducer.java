package com.example.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.example.mq.delay.DelayMessageSender;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
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
    private final RabbitAdmin rabbitAdmin;
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;

    public RabbitMQProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitAdmin = new RabbitAdmin(rabbitTemplate.getConnectionFactory());
    }

    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }

        try {
            // 确保Exchange存在
            DirectExchange exchange = new DirectExchange(topic, true, false);
            rabbitAdmin.declareExchange(exchange);
            
            String messageId = "rabbitmq-" + System.currentTimeMillis();
            Message message = new Message(JSON.toJSONString(event).getBytes());
            MessageProperties properties = message.getMessageProperties();
            properties.setMessageId(messageId);
            if (tag != null) {
                properties.setHeader("tag", tag);
            }
            
            String routingKey = tag != null ? tag : "";
            rabbitTemplate.send(topic, routingKey, message);
            log.info("RabbitMQ同步发送消息成功: topic={}, tag={}, messageId={}", topic, tag, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("RabbitMQ同步发送消息失败: topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("RabbitMQ发送消息失败", e);
        }
    }

    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }
        
        CompletableFuture.runAsync(() -> {
            try {
                // 确保Exchange存在
                DirectExchange exchange = new DirectExchange(topic, true, false);
                rabbitAdmin.declareExchange(exchange);
                
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
                
                String routingKey = tag != null ? tag : "";
                rabbitTemplate.send(topic, routingKey, message);
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
    public String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }

        try {
            // 确保FanoutExchange存在用于广播
            FanoutExchange exchange = new FanoutExchange(topic + ".broadcast", true, false);
            rabbitAdmin.declareExchange(exchange);
            
            String messageId = "rabbitmq-broadcast-" + System.currentTimeMillis();
            Message message = new Message(JSON.toJSONString(event).getBytes());
            MessageProperties properties = message.getMessageProperties();
            properties.setMessageId(messageId);
            if (tag != null) {
                properties.setHeader("tag", tag);
            }
            
            // FanoutExchange不需要routing key，使用空字符串
            rabbitTemplate.send(topic + ".broadcast", "", message);
            log.info("RabbitMQ同步广播发送消息成功: topic={}, tag={}, messageId={}", topic, tag, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("RabbitMQ同步广播发送消息失败: topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("RabbitMQ广播发送消息失败", e);
        }
    }

    @Override
    public void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }
        
        CompletableFuture.runAsync(() -> {
            try {
                // 确保FanoutExchange存在用于广播
                FanoutExchange exchange = new FanoutExchange(topic + ".broadcast", true, false);
                rabbitAdmin.declareExchange(exchange);
                
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
                
                // FanoutExchange不需要routing key，使用空字符串
                rabbitTemplate.send(topic + ".broadcast", "", message);
                log.info("RabbitMQ异步广播发送消息成功: topic={}, tag={}", topic, tag);
            } catch (Exception e) {
                log.error("RabbitMQ异步广播发送消息失败: topic={}, tag={}", topic, tag, e);
            }
        });
    }

    @Override
    public String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            return null;
        }
        // 广播延迟消息使用特殊的topic标识
        String broadcastTopic = topic + ".broadcast";
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(broadcastTopic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.RABBIT_MQ;
    }
}