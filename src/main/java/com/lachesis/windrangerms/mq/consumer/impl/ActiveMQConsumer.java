package com.lachesis.windrangerms.mq.consumer.impl;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * ActiveMQ消费者实现
 */
@Component
@ConditionalOnBean(JmsTemplate.class)
public class ActiveMQConsumer implements MQConsumer {

    private static final Logger log = LoggerFactory.getLogger(ActiveMQConsumer.class);

    private final JmsTemplate jmsTemplate;
    private final Map<String, Consumer<String>> handlerMap = new ConcurrentHashMap<>();
    private final Map<String, DefaultMessageListenerContainer> containerMap = new ConcurrentHashMap<>();
    private final Map<String, Consumer<String>> broadcastHandlerMap = new ConcurrentHashMap<>();
    private final Map<String, DefaultMessageListenerContainer> broadcastContainerMap = new ConcurrentHashMap<>();

    @Autowired(required = false)
    private DeadLetterServiceFactory deadLetterServiceFactory;

    public ActiveMQConsumer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.ACTIVE_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected ACTIVE_MQ, got " + mqType);
        }

        try {
            // 默认订阅单播消息
            subscribeUnicastInternal(topic, tag, handler);
            
            log.info("ActiveMQ订阅成功: topic={}, tag={}", topic, tag);
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
            
            // ActiveMQ默认使用Queue模式，除非明确指定为Topic
            // 这里统一使用Queue模式，确保消息能够被正确消费
            boolean isTopicDestination = false;
            container.setPubSubDomain(isTopicDestination);
            
            // 添加更多配置确保消息监听容器正常工作
            container.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
            container.setConcurrentConsumers(1);
            container.setMaxConcurrentConsumers(1);
            container.setReceiveTimeout(1000);
            container.setIdleTaskExecutionLimit(1);
            container.setAutoStartup(true);
            
            log.info("ActiveMQ单播订阅配置: topic={}, tag={}, destination={}, pubSubDomain={}", topic, tag, destination, isTopicDestination);
            
            // 创建一个具体的MessageListener实现，而不是匿名类
            MessageListener messageListener = new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    String messageBody = null;
                    String messageId = null;
                    try {
                        if (message instanceof TextMessage) {
                            messageBody = ((TextMessage) message).getText();
                            messageId = message.getJMSMessageID();
                            log.info("ActiveMQ接收到消息: topic={}, tag={}, messageId={}", topic, tag, messageId);
                            
                            handler.accept(messageBody);
                            log.debug("ActiveMQ单播消息处理成功: topic={}, tag={}", topic, tag);
                        } else {
                            log.warn("ActiveMQ接收到非文本消息: topic={}, tag={}, messageType={}", topic, tag, message.getClass().getSimpleName());
                        }
                    } catch (JMSException e) {
                        log.error("ActiveMQ单播消息处理失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                        handleDeadLetter(messageId, topic, tag, messageBody, e.getMessage());
                    } catch (Exception e) {
                        log.error("ActiveMQ单播消息业务处理失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                        handleDeadLetter(messageId, topic, tag, messageBody, e.getMessage());
                    }
                }
            };
            
            container.setMessageListener(messageListener);
            container.start();
            containerMap.put(key, container);
            
            // 等待一下让容器完全启动
            Thread.sleep(1000);
            
            // 如果容器没有活跃消费者，尝试手动初始化
            if (container.getActiveConsumerCount() == 0) {
                log.debug("检测到无活跃消费者，尝试手动初始化");
                container.initialize();
                Thread.sleep(500);
            }
            
            // 测试连接
            testConnection(destination, isTopicDestination);
            
            log.info("ActiveMQ单播订阅成功: topic={}, tag={}, destination={}", topic, tag, destination);
        } catch (Exception e) {
            log.error("ActiveMQ单播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("ActiveMQ单播订阅失败: " + e.getMessage(), e);
        }
    }

    /**
     * 测试连接
     */
    private void testConnection(String destination, boolean isTopicDestination) {
        try {
            javax.jms.Connection connection = jmsTemplate.getConnectionFactory().createConnection();
            connection.start();
            javax.jms.Session session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
            
            if (isTopicDestination) {
                javax.jms.Topic jmsTopic = session.createTopic(destination);
                log.debug("成功创建Topic对象: {}", jmsTopic.getTopicName());
                log.debug("Topic连接测试完成");
            } else {
                javax.jms.Queue queue = session.createQueue(destination);
                log.debug("成功创建队列对象: {}", queue.getQueueName());
                
                // 尝试检查队列是否存在
                javax.jms.QueueBrowser browser = session.createBrowser(queue);
                log.debug("成功创建队列浏览器，队列存在: {}", destination);
                browser.close();
                log.debug("队列连接测试完成");
            }
            
            session.close();
            connection.close();
        } catch (Exception e) {
            log.warn("连接测试失败: {}", e.getMessage(), e);
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
                    String messageBody = null;
                    String messageId = null;
                    try {
                        if (message instanceof TextMessage) {
                            messageBody = ((TextMessage) message).getText();
                            messageId = message.getJMSMessageID();
                            handler.accept(messageBody);
                            log.debug("ActiveMQ广播消息处理成功: topic={}, tag={}, message={}, consumerKey={}", 
                                    topic, tag, messageBody, consumerKey);
                        }
                    } catch (JMSException e) {
                        log.error("ActiveMQ广播消息处理失败: topic={}, tag={}, consumerKey={}, error={}", 
                                topic, tag, consumerKey, e.getMessage(), e);
                        handleDeadLetter(messageId, topic, tag, messageBody, e.getMessage());
                    } catch (Exception e) {
                        log.error("ActiveMQ广播消息业务处理失败: topic={}, tag={}, consumerKey={}, error={}", 
                                topic, tag, consumerKey, e.getMessage(), e);
                        handleDeadLetter(messageId, topic, tag, messageBody, e.getMessage());
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

    /**
     * 处理死信消息
     */
    private void handleDeadLetter(String messageId, String topic, String tag, String messageBody, String errorMessage) {
        if (deadLetterServiceFactory == null) {
            log.warn("死信服务工厂未配置，无法处理死信消息: messageId={}, topic={}, tag={}", messageId, topic, tag);
            return;
        }

        try {
            DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
            if (deadLetterService == null) {
                log.warn("死信服务未配置，无法处理死信消息: messageId={}, topic={}, tag={}", messageId, topic, tag);
                return;
            }

            DeadLetterMessage deadLetterMessage = new DeadLetterMessage();
             deadLetterMessage.setOriginalMessageId(messageId != null ? messageId : java.util.UUID.randomUUID().toString());
             deadLetterMessage.setMqType("ACTIVE_MQ");
             deadLetterMessage.setOriginalTopic(topic);
             deadLetterMessage.setOriginalTag(tag);
             deadLetterMessage.setOriginalBody(messageBody);
             deadLetterMessage.setDeadLetterTime(System.currentTimeMillis());
             deadLetterMessage.setFailureReason(errorMessage);
             deadLetterMessage.setCreateTimestamp(System.currentTimeMillis());
             deadLetterMessage.setUpdateTimestamp(System.currentTimeMillis());
             deadLetterMessage.setStatus(0); // 待处理状态
             deadLetterMessage.setRetryCount(0);
             deadLetterMessage.setMaxRetryCount(3);

            // 设置消息属性
            Map<String, String> properties = new HashMap<>();
            properties.put("messageId", deadLetterMessage.getOriginalMessageId());
            properties.put("mqType", "ACTIVE_MQ");
            properties.put("topic", topic);
            properties.put("tag", tag != null ? tag : "");
            deadLetterMessage.setProperties(properties);

            boolean saved = deadLetterService.saveDeadLetterMessage(deadLetterMessage);
            if (saved) {
                log.info("ActiveMQ死信消息保存成功: messageId={}, topic={}, tag={}", 
                    deadLetterMessage.getOriginalMessageId(), topic, tag);
            } else {
                log.error("ActiveMQ死信消息保存失败: messageId={}, topic={}, tag={}", 
                    deadLetterMessage.getOriginalMessageId(), topic, tag);
            }
        } catch (Exception e) {
            log.error("处理ActiveMQ死信消息时发生异常: messageId={}, topic={}, tag={}, error={}", 
                messageId, topic, tag, e.getMessage(), e);
        }
    }
}