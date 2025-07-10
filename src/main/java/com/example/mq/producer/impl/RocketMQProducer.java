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
    public void asyncSendDelay(MQTypeEnum mqType, String topic, String tag, MQEvent event, int delaySecond) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        try {
            String message = JSON.toJSONString(event);
            log.info("RocketMQ异步发送延迟消息：topic={}, tag={}, message={}, delaySecond={}", topic, tag, message, delaySecond);
            Message msg = new Message(topic, tag, message.getBytes());
            
            // 添加用户自定义属性
            if (event.getUserProperties() != null) {
                event.getUserProperties().forEach(msg::putUserProperty);
            }
            
            // 设置延迟级别
            msg.setDelayTimeLevel(getDelayTimeLevel(delaySecond));
            
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("RocketMQ异步发送延迟消息成功：topic={}, tag={}, msgId={}", topic, tag, sendResult.getMsgId());
                    if (event.getSendSuccessTime() != null) {
                        event.getSendSuccessTime().set(System.currentTimeMillis());
                    }
                }

                @Override
                public void onException(Throwable e) {
                    log.error("RocketMQ异步发送延迟消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
                }
            });
        } catch (Exception e) {
            log.error("RocketMQ异步发送延迟消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
        }
    }

    /**
     * 将延迟秒数转换为RocketMQ的延迟级别
     */
    private int getDelayTimeLevel(int delaySecond) {
        // RocketMQ的延迟级别：1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        if (delaySecond <= 1) {
            return 1;
        } else if (delaySecond <= 5) {
            return 2;
        } else if (delaySecond <= 10) {
            return 3;
        } else if (delaySecond <= 30) {
            return 4;
        } else if (delaySecond <= 60) {
            return 5;
        } else if (delaySecond <= 120) {
            return 6;
        } else if (delaySecond <= 180) {
            return 7;
        } else if (delaySecond <= 240) {
            return 8;
        } else if (delaySecond <= 300) {
            return 9;
        } else if (delaySecond <= 360) {
            return 10;
        } else if (delaySecond <= 420) {
            return 11;
        } else if (delaySecond <= 480) {
            return 12;
        } else if (delaySecond <= 540) {
            return 13;
        } else if (delaySecond <= 600) {
            return 14;
        } else if (delaySecond <= 1200) {
            return 15;
        } else if (delaySecond <= 1800) {
            return 16;
        } else if (delaySecond <= 3600) {
            return 17;
        } else {
            return 18;
        }
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.ROCKET_MQ;
    }
}