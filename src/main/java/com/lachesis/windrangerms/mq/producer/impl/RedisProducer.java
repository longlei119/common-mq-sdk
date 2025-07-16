package com.lachesis.windrangerms.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisProducer implements MQProducer {

    private final StringRedisTemplate redisTemplate;
    private final DelayMessageSender delayMessageSender;

    public RedisProducer(StringRedisTemplate redisTemplate, DelayMessageSender delayMessageSender) {
        this.redisTemplate = redisTemplate;
        this.delayMessageSender = delayMessageSender;
    }

    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.REDIS) {
            return null;
        }

        String channel = topic + ":" + tag;
        String messageId = UUID.randomUUID().toString();
        String message = JSON.toJSONString(event);

        try {
            redisTemplate.convertAndSend(channel, message);
            log.info("Redis消息发送成功 - channel: {}, messageId: {}, message: {}", channel, messageId, message);
            return messageId;
        } catch (Exception e) {
            log.error("Redis消息发送失败 - channel: {}, messageId: {}, message: {}, error: {}", 
                    channel, messageId, message, e.getMessage());
            throw new RuntimeException("Redis消息发送失败", e);
        }
    }

    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }

        String channel = topic + ":" + tag;
        String message = event instanceof String ? (String) event : JSON.toJSONString(event);
        CompletableFuture.runAsync(() -> {
            try {
                redisTemplate.convertAndSend(channel, message);
                log.info("Redis消息发送成功 - channel: {}, message: {}", channel, message);
            } catch (Exception e) {
                log.error("Redis消息发送失败 - channel: {}, message: {}, error: {}", channel, message, e.getMessage());
                throw new RuntimeException("Redis消息发送失败", e);
            }
        });
    }

    @Override
    public String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.REDIS) {
            return null;
        }
        
        if (delayMessageSender == null) {
            throw new RuntimeException("DelayMessageSender未配置，请检查mq.delay.enabled配置和Redis连接");
        }
        
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(topic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.REDIS) {
            return null;
        }

        String broadcastChannel = topic + ".broadcast:" + (tag != null ? tag : "");
        String messageId = UUID.randomUUID().toString();
        String message = JSON.toJSONString(event);

        try {
            redisTemplate.convertAndSend(broadcastChannel, message);
            log.info("Redis广播消息发送成功 - channel: {}, messageId: {}, message: {}", broadcastChannel, messageId, message);
            return messageId;
        } catch (Exception e) {
            log.error("Redis广播消息发送失败 - channel: {}, messageId: {}, message: {}, error: {}", 
                    broadcastChannel, messageId, message, e.getMessage());
            throw new RuntimeException("Redis广播消息发送失败", e);
        }
    }

    @Override
    public void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }

        String broadcastChannel = topic + ".broadcast:" + (tag != null ? tag : "");
        String message = event instanceof String ? (String) event : JSON.toJSONString(event);
        CompletableFuture.runAsync(() -> {
            try {
                redisTemplate.convertAndSend(broadcastChannel, message);
                log.info("Redis广播消息发送成功 - channel: {}, message: {}", broadcastChannel, message);
            } catch (Exception e) {
                log.error("Redis广播消息发送失败 - channel: {}, message: {}, error: {}", broadcastChannel, message, e.getMessage());
                throw new RuntimeException("Redis广播消息发送失败", e);
            }
        });
    }

    @Override
    public String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.REDIS) {
            return null;
        }
        
        if (delayMessageSender == null) {
            throw new RuntimeException("DelayMessageSender未配置，请检查mq.delay.enabled配置和Redis连接");
        }
        
        // 广播延迟消息使用特殊的topic标识
        String broadcastTopic = topic + ".broadcast";
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(broadcastTopic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.REDIS;
    }
}