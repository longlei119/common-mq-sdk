package com.example.mq.consumer.impl;

import com.example.mq.consumer.MQConsumer;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * ActiveMQ消费者实现
 */
@Slf4j
@Component
@ConditionalOnBean(JmsTemplate.class)
public class ActiveMQConsumer implements MQConsumer {

    private final JmsTemplate jmsTemplate;
    private final Map<String, Consumer<String>> handlerMap = new ConcurrentHashMap<>();
    private final Map<String, DefaultMessageListenerContainer> containerMap = new ConcurrentHashMap<>();

    public ActiveMQConsumer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected ACTIVE_MQ, got " + mqType);
        }

        try {
            String destination = buildDestination(topic, tag);
            String key = topic + ":" + (tag != null ? tag : "");
            
            handlerMap.put(key, handler);
            
            // 创建消息监听容器
            DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
            container.setConnectionFactory(jmsTemplate.getConnectionFactory());
            container.setDestinationName(destination);
            container.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        if (message instanceof TextMessage) {
                            String messageBody = ((TextMessage) message).getText();
                            handler.accept(messageBody);
                            log.debug("ActiveMQ消息处理成功: topic={}, tag={}, message={}", topic, tag, messageBody);
                        }
                    } catch (JMSException e) {
                        log.error("ActiveMQ消息处理失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                    }
                }
            });
            
            container.start();
            containerMap.put(key, container);
            
            log.info("ActiveMQ订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("ActiveMQ订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ订阅失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected ACTIVE_MQ, got " + mqType);
        }

        try {
            String key = topic + ":" + (tag != null ? tag : "");
            
            // 停止并移除监听容器
            DefaultMessageListenerContainer container = containerMap.remove(key);
            if (container != null) {
                container.stop();
                container.destroy();
            }
            
            handlerMap.remove(key);
            
            log.info("ActiveMQ取消订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("ActiveMQ取消订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ取消订阅失败", e);
        }
    }

    @Override
    public void start() {
        log.info("ActiveMQ消费者启动");
        // 容器在订阅时已经启动，这里可以做一些初始化工作
    }

    @Override
    public void stop() {
        log.info("ActiveMQ消费者停止");
        destroy();
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

    /**
     * 销毁方法，停止所有监听容器
     */
    public void destroy() {
        containerMap.values().forEach(container -> {
            try {
                container.stop();
                container.destroy();
            } catch (Exception e) {
                log.warn("停止ActiveMQ监听容器失败: {}", e.getMessage());
            }
        });
        containerMap.clear();
        handlerMap.clear();
    }
}