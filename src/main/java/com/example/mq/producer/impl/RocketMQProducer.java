package com.example.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * RocketMQ生产者实现
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
public class RocketMQProducer implements MQProducer {

    private final DefaultMQProducer producer;

    public RocketMQProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return null;
        }
        try {
            String message = JSON.toJSONString(event);
            log.info("RocketMQ同步发送消息：topic={}, tag={}, message={}", topic, tag, message);
            Message msg = new Message(topic, tag, message.getBytes());
            SendResult sendResult = producer.send(msg);
            return sendResult.getMsgId();
        } catch (Exception e) {
            log.error("RocketMQ同步发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
            throw new RuntimeException("发送消息失败", e);
        }
    }

    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        try {
            String message = event instanceof String ? (String) event : JSON.toJSONString(event);
            log.info("RocketMQ异步发送消息：topic={}, tag={}, message={}", topic, tag, message);
            Message msg = new Message(topic, tag, message.getBytes());
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("RocketMQ异步发送消息成功：topic={}, tag={}, msgId={}", topic, tag, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable throwable) {
                    log.error("RocketMQ异步发送消息失败：topic={}, tag={}", topic, tag, throwable);
                }
            });
        } catch (Exception e) {
            log.error("RocketMQ异步发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
            throw new RuntimeException("发送消息失败", e);
        }
    }

    @Override
    public void asyncSendDelay(MQTypeEnum mqType, String topic, String tag, MQEvent event, int delaySecond) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        try {
            String message = JSON.toJSONString(event);
            log.info("RocketMQ异步发送延迟消息：topic={}, tag={}, message={}, delaySecond={}", topic, tag, message, delaySecond);
            Message msg = new Message(topic, tag, message.getBytes());
            // 设置延迟级别
            int delayLevel = convertToDelayLevel(delaySecond);
            msg.setDelayTimeLevel(delayLevel);
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    String sendSuccessTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date());
                    log.info("RocketMQ异步发送延迟消息成功：topic={}, tag={}, msgId={}, delaySecond={}, sendSuccessTime={}", 
                            topic, tag, sendResult.getMsgId(), delaySecond, sendSuccessTime);
                }

                @Override
                public void onException(Throwable throwable) {
                    log.error("RocketMQ异步发送延迟消息失败：topic={}, tag={}, delaySecond={}", 
                            topic, tag, delaySecond, throwable);
                }
            });
        } catch (Exception e) {
            log.error("RocketMQ异步发送延迟消息失败：topic={}, tag={}, event={}, delaySecond={}", 
                    topic, tag, event, delaySecond, e);
            throw new RuntimeException("发送延迟消息失败", e);
        }
    }

    /**
     * 将秒级延迟转换为RocketMQ的延迟级别
     * RocketMQ的延迟级别为：1s、5s、10s、30s、1m、2m、3m、4m、5m、6m、7m、8m、9m、10m、20m、30m、1h、2h
     */
    private int convertToDelayLevel(int delaySecond) {
        if (delaySecond <= 1) return 1;      // 1s
        if (delaySecond <= 5) return 2;      // 5s
        if (delaySecond <= 10) return 3;     // 10s
        if (delaySecond <= 30) return 4;     // 30s
        if (delaySecond <= 60) return 5;     // 1m
        if (delaySecond <= 120) return 6;    // 2m
        if (delaySecond <= 180) return 7;    // 3m
        if (delaySecond <= 240) return 8;    // 4m
        if (delaySecond <= 300) return 9;    // 5m
        if (delaySecond <= 360) return 10;   // 6m
        if (delaySecond <= 420) return 11;   // 7m
        if (delaySecond <= 480) return 12;   // 8m
        if (delaySecond <= 540) return 13;   // 9m
        if (delaySecond <= 600) return 14;   // 10m
        if (delaySecond <= 1200) return 15;  // 20m
        if (delaySecond <= 1800) return 16;  // 30m
        if (delaySecond <= 3600) return 17;  // 1h
        return 18;                           // 2h
    }
}