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
            String destination = buildDestination(topic, tag);
            
            // 判断是否为Topic类型的destination，与消费者逻辑保持一致
            // ActiveMQ默认使用Queue模式，确保与消费者端保持一致
        boolean isTopicDestination = false;
            jmsTemplate.setPubSubDomain(isTopicDestination);
            
            jmsTemplate.convertAndSend(destination, JSON.toJSONString(event));
            log.info("ActiveMQ同步发送消息成功: topic={}, tag={}, destination={}, messageId={}, pubSubDomain={}", topic, tag, destination, messageId, jmsTemplate.isPubSubDomain());
            
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
                String destination = buildDestination(topic, tag);
                
                // ActiveMQ默认使用Queue模式，确保与消费者端保持一致
                boolean isTopicDestination = false;
                jmsTemplate.setPubSubDomain(isTopicDestination);
                
                jmsTemplate.convertAndSend(destination, event);
                log.info("ActiveMQ异步发送消息成功: topic={}, tag={}, destination={}", topic, tag, destination);
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
        String destination = buildDestination(topic, tag);
        // ActiveMQ默认使用Queue模式，确保与消费者端保持一致
        boolean isTopicDestination = false;
        
        try {
            log.debug("开始ActiveMQ消息发送: topic={}, tag={}, destination={}", topic, tag, destination);
            log.debug("消息内容: {}", body);
            log.debug("消息属性: {}", properties);
            
            // 判断是否为Topic类型的destination
            if (isTopicDestination) {
                log.debug("检测到Topic类型destination，设置pubSubDomain=true");
                jmsTemplate.setPubSubDomain(true);
            } else {
                log.debug("检测到Queue类型destination，设置pubSubDomain=false");
                jmsTemplate.setPubSubDomain(false);
            }
            
            log.info("ActiveMQ发送消息：topic={}, tag={}, body={}", topic, tag, body);
            log.debug("ActiveMQ发送消息到destination: {}, pubSubDomain={}", destination, jmsTemplate.isPubSubDomain());

            log.debug("正在通过JmsTemplate发送消息...");
            jmsTemplate.convertAndSend(destination, body, message -> {
                log.debug("JMS消息创建成功，正在设置属性");
                // 添加用户自定义属性
                if (properties != null) {
                    log.debug("正在设置消息属性...");
                    properties.forEach((key, value) -> {
                        try {
                            message.setStringProperty(key, value);
                            log.debug("设置属性: {}={}", key, value);
                        } catch (Exception e) {
                            log.warn("设置消息属性失败: key={}, value={}, error={}", key, value, e.getMessage());
                        }
                    });
                    log.debug("消息属性设置完成");
                }
                log.debug("返回准备发送的消息: {}", message);
                return message;
            });

            log.debug("JmsTemplate.convertAndSend()调用完成");
            log.debug("ActiveMQ消息发送完成");
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