package com.example.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.example.mq.delay.DelayMessageSender;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;

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
            
            // 添加用户自定义属性
            if (event.getUserProperties() != null) {
                event.getUserProperties().forEach(msg::putUserProperty);
            }
            
            SendResult sendResult = producer.send(msg);
            return sendResult.getMsgId();
        } catch (Exception e) {
            log.error("RocketMQ同步发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
            return null;
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
            
            // 添加用户自定义属性
            if (event instanceof MQEvent) {
                MQEvent mqEvent = (MQEvent) event;
                if (mqEvent.getUserProperties() != null) {
                    mqEvent.getUserProperties().forEach(msg::putUserProperty);
                }
            }
            
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("RocketMQ异步发送消息成功：topic={}, tag={}, msgId={}", topic, tag, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    log.error("RocketMQ异步发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
                }
            });
        } catch (Exception e) {
            log.error("RocketMQ异步发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
        }
    }

    @Override
    public String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return null;
        }
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(topic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return null;
        }
        try {
            String message = JSON.toJSONString(event);
            log.info("RocketMQ同步广播发送消息：topic={}, tag={}, message={}", topic, tag, message);
            Message msg = new Message(topic, tag, message.getBytes());
            
            // 添加用户自定义属性
            if (event.getUserProperties() != null) {
                event.getUserProperties().forEach(msg::putUserProperty);
            }
            
            // 广播消息不需要特殊处理，RocketMQ的广播模式由消费者端的MessageModel决定
            SendResult sendResult = producer.send(msg);
            log.info("RocketMQ同步广播发送消息成功：topic={}, tag={}, msgId={}", topic, tag, sendResult.getMsgId());
            return sendResult.getMsgId();
        } catch (Exception e) {
            log.error("RocketMQ同步广播发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
            return null;
        }
    }

    @Override
    public void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        try {
            String message = event instanceof String ? (String) event : JSON.toJSONString(event);
            log.info("RocketMQ异步广播发送消息：topic={}, tag={}, message={}", topic, tag, message);
            Message msg = new Message(topic, tag, message.getBytes());
            
            // 添加用户自定义属性
            if (event instanceof MQEvent) {
                MQEvent mqEvent = (MQEvent) event;
                if (mqEvent.getUserProperties() != null) {
                    mqEvent.getUserProperties().forEach(msg::putUserProperty);
                }
            }
            
            // 广播消息不需要特殊处理，RocketMQ的广播模式由消费者端的MessageModel决定
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("RocketMQ异步广播发送消息成功：topic={}, tag={}, msgId={}", topic, tag, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    log.error("RocketMQ异步广播发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
                }
            });
        } catch (Exception e) {
            log.error("RocketMQ异步广播发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
        }
    }

    @Override
    public String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return null;
        }
        // 广播延迟消息使用特殊的topic标识
        String broadcastTopic = topic + ".broadcast";
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(broadcastTopic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.ROCKET_MQ;
    }
}