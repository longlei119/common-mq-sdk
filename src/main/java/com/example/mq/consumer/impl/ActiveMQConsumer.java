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
    private final Map<String, Consumer<String>> broadcastHandlerMap = new ConcurrentHashMap<>();
    private final Map<String, DefaultMessageListenerContainer> broadcastContainerMap = new ConcurrentHashMap<>();

    public ActiveMQConsumer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected ACTIVE_MQ, got " + mqType);
        }

        try {
            // 同时订阅单播和广播消息
            subscribeUnicastInternal(topic, tag, handler);
            subscribeBroadcastInternal(topic, tag, handler);
            
            log.info("ActiveMQ统一订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("ActiveMQ订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ订阅失败", e);
        }
    }
    
    @Override
    public void subscribeUnicast(String topic, String tag, Consumer<String> handler, String consumerGroup) {
        subscribeUnicastInternal(topic, tag, handler);
    }

    @Override
    public void subscribeBroadcast(String topic, String tag, Consumer<String> handler) {
        subscribeBroadcastInternal(topic, tag, handler);
    }

    @Override
    public void subscribeBroadcast(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            return;
        }
        subscribeBroadcastInternal(topic, tag, handler);
    }

    /**
     * 单播订阅的内部实现
     */
    private void subscribeUnicastInternal(String topic, String tag, Consumer<String> handler) {
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
                            log.debug("ActiveMQ单播消息处理成功: topic={}, tag={}, message={}", topic, tag, messageBody);
                        }
                    } catch (JMSException e) {
                        log.error("ActiveMQ单播消息处理失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                    }
                }
            });
            
            container.start();
            containerMap.put(key, container);
            
            log.debug("ActiveMQ单播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("ActiveMQ单播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ单播订阅失败", e);
        }
    }

    /**
     * 广播订阅的内部实现
     */
    private void subscribeBroadcastInternal(String topic, String tag, Consumer<String> handler) {
        MQTypeEnum mqType = MQTypeEnum.ACTIVE_MQ;
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected ACTIVE_MQ, got " + mqType);
        }

        try {
            // 广播模式使用Topic类型的destination，添加UUID确保每个消费者独立
            String broadcastDestination = "topic://" + topic + ".broadcast" + (tag != null ? "." + tag : "");
            String consumerKey = topic + ":" + (tag != null ? tag : "") + ":" + java.util.UUID.randomUUID().toString();
            
            broadcastHandlerMap.put(consumerKey, handler);
            
            // 创建消息监听容器
            DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
            container.setConnectionFactory(jmsTemplate.getConnectionFactory());
            container.setDestinationName(broadcastDestination);
            container.setPubSubDomain(true); // 设置为Topic模式（广播）
            container.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        if (message instanceof TextMessage) {
                            String messageBody = ((TextMessage) message).getText();
                            handler.accept(messageBody);
                            log.debug("ActiveMQ广播消息处理成功: topic={}, tag={}, message={}, consumerKey={}", 
                                    topic, tag, messageBody, consumerKey);
                        }
                    } catch (JMSException e) {
                        log.error("ActiveMQ广播消息处理失败: topic={}, tag={}, consumerKey={}, error={}", 
                                topic, tag, consumerKey, e.getMessage(), e);
                    }
                }
            });
            
            container.start();
            broadcastContainerMap.put(consumerKey, container);
            
            log.info("ActiveMQ广播订阅成功: topic={}, tag={}, consumerKey={}", topic, tag, consumerKey);
        } catch (Exception e) {
            log.error("ActiveMQ广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ广播订阅失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            return;
        }
        
        try {
            // 取消单播订阅
            unsubscribeUnicastInternal(topic, tag);
            
            // 取消广播订阅
            unsubscribeBroadcastInternal(topic, tag);
            
            log.info("ActiveMQ取消订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("ActiveMQ取消订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ取消订阅失败", e);
        }
    }
    
    @Override
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            return;
        }
        
        try {
            unsubscribeBroadcastInternal(topic, tag);
            log.info("ActiveMQ取消广播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("ActiveMQ取消广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ取消广播订阅失败", e);
        }
    }
    
    /**
     * 取消单播订阅的内部实现
     */
    private void unsubscribeUnicastInternal(String topic, String tag) {
        String key = topic + ":" + (tag != null ? tag : "");
        
        // 停止并移除监听容器
        DefaultMessageListenerContainer container = containerMap.remove(key);
        if (container != null) {
            container.stop();
            container.destroy();
        }
        
        handlerMap.remove(key);
        
        log.debug("ActiveMQ取消单播订阅成功: topic={}, tag={}", topic, tag);
    }
    
    /**
     * 取消广播订阅的内部实现
     */
    private void unsubscribeBroadcastInternal(String topic, String tag) {
        String keyPrefix = topic + ":" + (tag != null ? tag : "") + ":";
        
        // 移除所有匹配的广播订阅
        broadcastContainerMap.entrySet().removeIf(entry -> {
            if (entry.getKey().startsWith(keyPrefix)) {
                try {
                    entry.getValue().stop();
                    entry.getValue().destroy();
                    broadcastHandlerMap.remove(entry.getKey());
                    log.debug("ActiveMQ取消广播订阅成功: consumerKey={}", entry.getKey());
                    return true;
                } catch (Exception e) {
                    log.warn("停止ActiveMQ广播监听容器失败: consumerKey={}, error={}", entry.getKey(), e.getMessage());
                    return false;
                }
            }
            return false;
        });
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
        // 停止单播容器
        containerMap.values().forEach(container -> {
            try {
                container.stop();
                container.destroy();
                log.info("ActiveMQ单播监听容器停止成功");
            } catch (Exception e) {
                log.warn("停止ActiveMQ单播监听容器失败: {}", e.getMessage());
            }
        });
        
        // 停止广播容器
        broadcastContainerMap.values().forEach(container -> {
            try {
                container.stop();
                container.destroy();
                log.info("ActiveMQ广播监听容器停止成功");
            } catch (Exception e) {
                log.warn("停止ActiveMQ广播监听容器失败: {}", e.getMessage());
            }
        });
        
        // 清理资源
        containerMap.clear();
        handlerMap.clear();
        broadcastContainerMap.clear();
        broadcastHandlerMap.clear();
    }
}