package com.lachesis.windrangerms.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

/**
 * ActiveMQ生产者实现
 */
@Component
@ConditionalOnBean(JmsTemplate.class)
public class ActiveMQProducer implements MQProducer {

    private static final Logger log = LoggerFactory.getLogger(ActiveMQProducer.class);

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
    public String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        try {
            String messageId = "activemq-broadcast-" + System.currentTimeMillis();
            String broadcastDestination = "topic://" + topic + ".broadcast" + (tag != null ? "." + tag : "");

            // 设置为Topic模式（广播）
            jmsTemplate.setPubSubDomain(true);
            jmsTemplate.convertAndSend(broadcastDestination, JSON.toJSONString(event));
            // 恢复默认的Queue模式
            jmsTemplate.setPubSubDomain(false);

            log.info("ActiveMQ同步广播发送消息成功: topic={}, tag={}, messageId={}", topic, tag, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("ActiveMQ同步广播发送消息失败: topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("ActiveMQ广播发送消息失败", e);
        }
    }

    @Override
    public void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event) {
        CompletableFuture.runAsync(() -> {
            try {
                String broadcastDestination = "topic://" + topic + ".broadcast" + (tag != null ? "." + tag : "");

                // 设置为Topic模式（广播）
                jmsTemplate.setPubSubDomain(true);
                jmsTemplate.convertAndSend(broadcastDestination, event);
                // 恢复默认的Queue模式
                jmsTemplate.setPubSubDomain(false);

                log.info("ActiveMQ异步广播发送消息成功: topic={}, tag={}", topic, tag);
            } catch (Exception e) {
                log.error("ActiveMQ异步广播发送消息失败: topic={}, tag={}", topic, tag, e);
            }
        });
    }

    @Override
    public String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            return null;
        }
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        String broadcastTopic = topic + ".broadcast";
        return delayMessageSender.sendDelayMessage(broadcastTopic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.ACTIVE_MQ;
    }

    @Override
    public boolean send(String topic, String tag, String body, Map<String, String> properties) {
        try {
            log.info("ActiveMQ发送消息：topic={}, tag={}, body={}", topic, tag, body);
            String destination = buildDestination(topic, tag);

            jmsTemplate.convertAndSend(destination, body, message -> {
                // 添加用户自定义属性
                if (properties != null) {
                    properties.forEach((key, value) -> {
                        try {
                            message.setStringProperty(key, value);
                        } catch (Exception e) {
                            log.warn("设置消息属性失败: key={}, value={}", key, value, e);
                        }
                    });
                }
                return message;
            });

            return true;
        } catch (Exception e) {
            log.error("ActiveMQ发送消息失败：topic={}, tag={}, body={}", topic, tag, body, e);
            return false;
        }
    }

    private String buildDestination(String topic, String tag) {
        if (tag != null && !tag.isEmpty()) {
            return topic + "." + tag;
        }
        return topic;
    }
}