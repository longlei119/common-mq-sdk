package com.example.mq.consumer.impl;

import com.example.mq.consumer.MQConsumer;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * RabbitMQ消费者实现
 */
@Slf4j
@Component
@ConditionalOnBean(RabbitTemplate.class)
public class RabbitMQConsumer implements MQConsumer {

    private final RabbitTemplate rabbitTemplate;
    private final ConnectionFactory connectionFactory;
    private final Map<String, Consumer<String>> handlerMap = new ConcurrentHashMap<>();
    private final Map<String, SimpleMessageListenerContainer> containerMap = new ConcurrentHashMap<>();

    public RabbitMQConsumer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        this.connectionFactory = rabbitTemplate.getConnectionFactory();
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }

        try {
            String key = topic + ":" + (tag != null ? tag : "");
            String queueName = buildQueueName(topic, tag);
            
            handlerMap.put(key, handler);
            
            // 创建消息监听容器
            SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
            container.setConnectionFactory(connectionFactory);
            container.setQueueNames(queueName);
            container.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        String messageBody = new String(message.getBody());
                        handler.accept(messageBody);
                        log.debug("RabbitMQ消息处理成功: topic={}, tag={}, message={}", topic, tag, messageBody);
                    } catch (Exception e) {
                        log.error("RabbitMQ消息处理失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                    }
                }
            });
            
            container.start();
            containerMap.put(key, container);
            
            log.info("RabbitMQ订阅成功: topic={}, tag={}, queue={}", topic, tag, queueName);
        } catch (Exception e) {
            log.error("RabbitMQ订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("RabbitMQ订阅失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }

        try {
            String key = topic + ":" + (tag != null ? tag : "");
            
            // 停止并移除监听容器
            SimpleMessageListenerContainer container = containerMap.remove(key);
            if (container != null) {
                container.stop();
                container.destroy();
            }
            
            handlerMap.remove(key);
            
            log.info("RabbitMQ取消订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("RabbitMQ取消订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("RabbitMQ取消订阅失败", e);
        }
    }

    @Override
    public void start() {
        log.info("RabbitMQ消费者启动");
        // 容器在订阅时已经启动，这里可以做一些初始化工作
    }

    @Override
    public void stop() {
        log.info("RabbitMQ消费者停止");
        for (Map.Entry<String, SimpleMessageListenerContainer> entry : containerMap.entrySet()) {
            entry.getValue().stop();
            log.info("RabbitMQ消费者停止成功: topic={}", entry.getKey());
        }
        containerMap.clear();
        handlerMap.clear();
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.RABBIT_MQ;
    }

    private String buildQueueName(String topic, String tag) {
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
                log.warn("停止RabbitMQ监听容器失败: {}", e.getMessage());
            }
        });
        containerMap.clear();
        handlerMap.clear();
    }
}