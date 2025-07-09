package com.example.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisProducer implements MQProducer {

    private final StringRedisTemplate redisTemplate;

    public RedisProducer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
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
    public void asyncSendDelay(MQTypeEnum mqType, String topic, String tag, MQEvent event, int delaySecond) {
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }

        String channel = topic + ":" + tag;
        String message = JSON.toJSONString(event);
        String delayKey = channel + ":delay:" + UUID.randomUUID().toString();

        CompletableFuture.runAsync(() -> {
            try {
                // 设置延迟键值对
                redisTemplate.opsForValue().set(delayKey, message, delaySecond, TimeUnit.SECONDS);
                log.info("Redis延迟消息设置成功 - channel: {}, delayKey: {}, delaySeconds: {}, message: {}",
                        channel, delayKey, delaySecond, message);

                // 等待延迟时间
                Thread.sleep(delaySecond * 1000L);

                // 发送消息
                redisTemplate.convertAndSend(channel, message);
                log.info("Redis延迟消息发送成功 - channel: {}, delayKey: {}, message: {}",
                        channel, delayKey, message);
            } catch (Exception e) {
                log.error("Redis延迟消息发送失败 - channel: {}, delayKey: {}, message: {}, error: {}",
                        channel, delayKey, message, e.getMessage());
                throw new RuntimeException("Redis延迟消息发送失败", e);
            } finally {
                // 清理延迟键
                redisTemplate.delete(delayKey);
            }
        });
    }
}