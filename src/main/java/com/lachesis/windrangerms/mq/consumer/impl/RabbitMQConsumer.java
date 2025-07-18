package com.lachesis.windrangerms.mq.consumer.impl;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import com.rabbitmq.client.Channel;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;

import java.util.HashMap;
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
    private final RabbitAdmin rabbitAdmin;
    private final Map<String, Consumer<String>> handlerMap = new ConcurrentHashMap<>();
    private final Map<String, SimpleMessageListenerContainer> containerMap = new ConcurrentHashMap<>();
    // 广播模式的处理器和容器映射
    private final Map<String, Consumer<String>> broadcastHandlerMap = new ConcurrentHashMap<>();
    private final Map<String, SimpleMessageListenerContainer> broadcastContainerMap = new ConcurrentHashMap<>();
    
    @Autowired(required = false)
    private DeadLetterServiceFactory deadLetterServiceFactory;

    public RabbitMQConsumer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        this.connectionFactory = rabbitTemplate.getConnectionFactory();
        this.rabbitAdmin = new RabbitAdmin(connectionFactory);
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }

        String key = topic + ":" + (tag != null ? tag : "");
        
        // 使用synchronized确保多线程环境下的订阅操作原子性
        synchronized (this) {
            try {
                // 检查是否已经订阅，如果是则先取消订阅
                if (containerMap.containsKey(key) || broadcastContainerMap.values().stream().anyMatch(c -> c.getQueueNames()[0].startsWith(buildQueueName(topic, tag)))) {
                    log.warn("重复订阅RabbitMQ消息，先取消旧订阅：topic={}, tag={}", topic, tag);
                    unsubscribeInternal(key);
                }
                
                // 只订阅单播消息，避免交换机类型冲突
                subscribeUnicastInternal(topic, tag, handler, key);
                
                log.info("RabbitMQ单播订阅成功: topic={}, tag={}", topic, tag);
            } catch (Exception e) {
                log.error("RabbitMQ订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                throw new RuntimeException("RabbitMQ订阅失败", e);
            }
        }
    }

    @Override
    public void subscribeUnicast(String topic, String tag, Consumer<String> handler, String consumerGroup) {
        String key = topic + ":" + (tag != null ? tag : "");
        synchronized (this) {
            subscribeUnicastInternal(topic, tag, handler, key);
        }
    }

    @Override
    public void subscribeBroadcast(String topic, String tag, Consumer<String> handler) {
        String key = topic + ":" + (tag != null ? tag : "");
        synchronized (this) {
            subscribeBroadcastInternal(topic, tag, handler, key);
        }
    }

    @Override
    public void subscribeBroadcast(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            return;
        }
        String key = topic + ":" + (tag != null ? tag : "");
        synchronized (this) {
            subscribeBroadcastInternal(topic, tag, handler, key);
        }
    }

    /**
     * 订阅单播消息的内部方法
     */
    private void subscribeUnicastInternal(String topic, String tag, Consumer<String> handler, String key) {
        try {
            String queueName = buildQueueName(topic, tag);
            String deadLetterQueueName = queueName + ".dlq";
            String deadLetterExchangeName = topic + ".dlx";
            
            // 声明DirectExchange用于单播
            DirectExchange exchange = new DirectExchange(topic, true, false);
            rabbitAdmin.declareExchange(exchange);
            
            // 声明死信交换机
            DirectExchange deadLetterExchange = new DirectExchange(deadLetterExchangeName, true, false);
            rabbitAdmin.declareExchange(deadLetterExchange);
            
            // 声明死信队列
            Queue deadLetterQueue = new Queue(deadLetterQueueName, true, false, false);
            rabbitAdmin.declareQueue(deadLetterQueue);
            
            // 绑定死信队列到死信交换机
            String routingKey = tag != null ? tag : "";
            Binding deadLetterBinding = BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(routingKey);
            rabbitAdmin.declareBinding(deadLetterBinding);
            
            // 尝试声明主队列，如果队列已存在且参数不匹配，则删除并重新创建
            Queue queue;
            try {
                Map<String, Object> args = new HashMap<>();
                args.put("x-dead-letter-exchange", deadLetterExchangeName);
                args.put("x-dead-letter-routing-key", routingKey);
                // 注释掉TTL设置，避免与已存在队列的参数冲突
                // args.put("x-message-ttl", 60000); // 消息TTL 60秒
                queue = new Queue(queueName, true, false, false, args);
                rabbitAdmin.declareQueue(queue);
            } catch (Exception e) {
                // 检查异常链中是否包含PRECONDITION_FAILED错误
                boolean isPreconditionFailed = false;
                Throwable cause = e;
                while (cause != null) {
                    if (cause.getMessage() != null && cause.getMessage().contains("PRECONDITION_FAILED")) {
                        isPreconditionFailed = true;
                        break;
                    }
                    cause = cause.getCause();
                }
                
                if (isPreconditionFailed) {
                    log.warn("队列 {} 已存在且参数不匹配，尝试删除并重新创建: {}", queueName, e.getMessage());
                    try {
                        // 删除现有队列
                        rabbitAdmin.deleteQueue(queueName);
                        log.info("已删除队列: {}", queueName);
                        
                        // 重新创建队列，带有正确的死信队列参数
                        Map<String, Object> args = new HashMap<>();
                        args.put("x-dead-letter-exchange", deadLetterExchangeName);
                        args.put("x-dead-letter-routing-key", routingKey);
                        queue = new Queue(queueName, true, false, false, args);
                        rabbitAdmin.declareQueue(queue);
                        log.info("已重新创建队列: {} 带有死信队列参数", queueName);
                    } catch (Exception deleteException) {
                        log.warn("删除队列失败，使用被动声明: {}", deleteException.getMessage());
                        // 如果删除失败，使用被动声明
                        queue = new Queue(queueName, true, false, false);
                        rabbitAdmin.declareQueue(queue);
                    }
                } else {
                    throw e;
                }
            }
            
            // 绑定队列到Exchange
            Binding binding = BindingBuilder.bind(queue).to(exchange).with(routingKey);
            rabbitAdmin.declareBinding(binding);
            
            handlerMap.put(key, handler);
            
            // 创建消息监听容器
            SimpleMessageListenerContainer container = createMessageListenerContainer(queueName, handler, topic, tag, "单播");
            containerMap.put(key, container);
            
            // 创建死信队列监听容器
            SimpleMessageListenerContainer deadLetterContainer = createDeadLetterMessageListenerContainer(deadLetterQueueName, topic, tag);
            containerMap.put(key + ":dlq", deadLetterContainer);
            
            log.debug("RabbitMQ单播订阅成功: topic={}, tag={}, queue={}", topic, tag, queueName);
        } catch (Exception e) {
            log.error("RabbitMQ单播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("RabbitMQ单播订阅失败", e);
        }
    }
    
    /**
     * 订阅广播消息的内部方法
     */
    private void subscribeBroadcastInternal(String topic, String tag, Consumer<String> handler, String baseKey) {
        try {
            // 广播模式使用UUID确保每个消费者有独立的队列
            String consumerKey = baseKey + ":" + java.util.UUID.randomUUID().toString();
            String queueName = buildBroadcastQueueName(topic, tag);
            
            // 声明FanoutExchange用于广播
            FanoutExchange exchange = new FanoutExchange(topic + ".broadcast", true, false);
            rabbitAdmin.declareExchange(exchange);
            
            // 声明临时队列（独占、自动删除）
            Queue queue = new Queue(queueName, false, true, true);
            rabbitAdmin.declareQueue(queue);
            
            // 绑定队列到FanoutExchange（广播模式不需要routing key）
            Binding binding = BindingBuilder.bind(queue).to(exchange);
            rabbitAdmin.declareBinding(binding);
            
            broadcastHandlerMap.put(consumerKey, handler);
            
            // 创建消息监听容器
            SimpleMessageListenerContainer container = createMessageListenerContainer(queueName, handler, topic, tag, "广播");
            broadcastContainerMap.put(consumerKey, container);
            
            log.debug("RabbitMQ广播订阅成功: topic={}, tag={}, queue={}, consumerKey={}", topic, tag, queueName, consumerKey);
        } catch (Exception e) {
            log.error("RabbitMQ广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("RabbitMQ广播订阅失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            throw new IllegalArgumentException("MQ type mismatch: expected RABBIT_MQ, got " + mqType);
        }

        String key = topic + ":" + (tag != null ? tag : "");
        String keyPrefix = key + ":";
        
        // 使用synchronized确保多线程环境下的取消订阅操作原子性
        synchronized (this) {
            try {
                // 取消单播订阅
                unsubscribeInternal(key);
                
                // 取消所有匹配的广播订阅
                broadcastContainerMap.entrySet().removeIf(entry -> {
                    if (entry.getKey().startsWith(keyPrefix)) {
                        try {
                            entry.getValue().stop();
                            entry.getValue().destroy();
                            broadcastHandlerMap.remove(entry.getKey());
                            return true;
                        } catch (Exception e) {
                            log.warn("停止RabbitMQ广播监听容器失败: key={}, error={}", entry.getKey(), e.getMessage());
                            return false;
                        }
                    }
                    return false;
                });
                
                log.info("RabbitMQ取消订阅成功: topic={}, tag={}", topic, tag);
            } catch (Exception e) {
                log.error("RabbitMQ取消订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                throw new RuntimeException("RabbitMQ取消订阅失败", e);
            }
        }
    }
    
    @Override
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.RABBIT_MQ) {
            return;
        }
        
        String key = topic + ":" + (tag != null ? tag : "");
        String keyPrefix = key + ":";
        
        synchronized (this) {
            try {
                // 取消所有匹配的广播订阅
                broadcastContainerMap.entrySet().removeIf(entry -> {
                    if (entry.getKey().startsWith(keyPrefix)) {
                        try {
                            entry.getValue().stop();
                            entry.getValue().destroy();
                            broadcastHandlerMap.remove(entry.getKey());
                            return true;
                        } catch (Exception e) {
                            log.warn("停止RabbitMQ广播监听容器失败: key={}, error={}", entry.getKey(), e.getMessage());
                            return false;
                        }
                    }
                    return false;
                });
                
                log.info("RabbitMQ取消广播订阅成功: topic={}, tag={}", topic, tag);
            } catch (Exception e) {
                log.error("RabbitMQ取消广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                throw new RuntimeException("RabbitMQ取消广播订阅失败", e);
            }
        }
    }
    
    /**
     * 创建消息监听容器的统一方法
     */
    private SimpleMessageListenerContainer createMessageListenerContainer(String queueName, Consumer<String> handler, String topic, String tag, String mode) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueName);
        
        // 设置手动确认模式，以支持消息重试
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        
        container.setMessageListener(new ChannelAwareMessageListener() {
            @Override
            public void onMessage(Message message, Channel channel) throws Exception {
                try {
                    String messageBody = new String(message.getBody());
                    
                    // 检查消息headers中是否包含messageId
                    String messageId = null;
                    if (message.getMessageProperties() != null && message.getMessageProperties().getHeaders() != null) {
                        Object messageIdObj = message.getMessageProperties().getHeaders().get("messageId");
                        if (messageIdObj != null) {
                            messageId = messageIdObj.toString();
                        }
                    }
                    
                    // 如果headers中有messageId，将其包装到消息体中
                    String finalMessage;
                    if (messageId != null) {
                        // 创建包含messageId的JSON格式消息
                        finalMessage = "{\"id\":\"" + messageId + "\",\"body\":\"" + messageBody.replace("\"", "\\\"") + "\"}";
                    } else {
                        finalMessage = messageBody;
                    }
                    
                    handler.accept(finalMessage);
                    // 手动确认消息
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                    log.debug("RabbitMQ{}消息处理成功: topic={}, tag={}, message={}", mode, topic, tag, finalMessage);
                } catch (Exception e) {
                    log.error("RabbitMQ{}消息处理失败: topic={}, tag={}, error={}", mode, topic, tag, e.getMessage(), e);
                    // 拒绝消息但不重新入队，让消息过期进入死信队列
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
                }
            }
        });
        
        // 设置容器启动超时时间
        container.setStartConsumerMinInterval(1000);
        container.setConsecutiveActiveTrigger(1);
        container.setConsecutiveIdleTrigger(1);
        
        container.start();
        
        // 等待容器完全启动
        int maxWaitTime = 5000; // 最多等待5秒
        int waitTime = 0;
        while (!container.isRunning() && waitTime < maxWaitTime) {
            try {
                Thread.sleep(100);
                waitTime += 100;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("等待容器启动被中断", e);
            }
        }
        
        if (!container.isRunning()) {
            throw new RuntimeException("RabbitMQ" + mode + "消息监听容器启动失败");
        }
        
        return container;
    }
    
    /**
     * 创建死信队列消息监听容器
     */
    private SimpleMessageListenerContainer createDeadLetterMessageListenerContainer(String deadLetterQueueName, String topic, String tag) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(deadLetterQueueName);
        
        // 设置手动确认模式
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        
        container.setMessageListener(new ChannelAwareMessageListener() {
            @Override
            public void onMessage(Message message, Channel channel) throws Exception {
                try {
                    String messageBody = new String(message.getBody());
                    
                    // 检查消息headers中是否包含messageId
                    String messageId = null;
                    if (message.getMessageProperties() != null && message.getMessageProperties().getHeaders() != null) {
                        Object messageIdObj = message.getMessageProperties().getHeaders().get("messageId");
                        if (messageIdObj != null) {
                            messageId = messageIdObj.toString();
                        }
                    }
                    
                    // 如果headers中有messageId，将其包装到消息体中
                    String finalMessage;
                    if (messageId != null) {
                        // 创建包含messageId的JSON格式消息
                        finalMessage = "{\"id\":\"" + messageId + "\",\"body\":\"" + messageBody.replace("\"", "\\\"") + "\"}";
                    } else {
                        finalMessage = messageBody;
                    }
                    
                    // 将消息发送到死信队列服务进行处理
                     log.info("RabbitMQ死信队列接收到消息: topic={}, tag={}, message={}", topic, tag, finalMessage);
                     
                     if (deadLetterServiceFactory != null) {
                         DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
                         if (deadLetterService != null) {
                             // 创建死信消息对象
                             com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage deadLetterMessage = new com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage();
                             deadLetterMessage.setId(java.util.UUID.randomUUID().toString().replace("-", ""));
                             deadLetterMessage.setOriginalMessageId(messageId);
                             deadLetterMessage.setTopic(topic);
                             deadLetterMessage.setTag(tag);
                             deadLetterMessage.setBody(finalMessage);
                             deadLetterMessage.setMqType(MQTypeEnum.RABBIT_MQ.name());
                             deadLetterMessage.setFailureReason("RabbitMQ消息处理失败");
                             deadLetterMessage.setRetryCount(0);
                             deadLetterMessage.setMaxRetryCount(3);
                             deadLetterMessage.setCreateTimestamp(System.currentTimeMillis());
                             deadLetterMessage.setUpdateTimestamp(System.currentTimeMillis());
                             
                             // 保存到死信队列服务中
                             boolean saved = deadLetterService.saveDeadLetterMessage(deadLetterMessage);
                             if (saved) {
                                 log.info("消息已添加到死信队列服务: messageId={}, topic={}, tag={}", messageId, topic, tag);
                             } else {
                                 log.error("保存消息到死信队列失败: messageId={}, topic={}, tag={}", messageId, topic, tag);
                             }
                         } else {
                             log.warn("死信队列服务未启用，无法处理死信消息: messageId={}, topic={}, tag={}", messageId, topic, tag);
                         }
                     } else {
                         log.warn("死信队列服务工厂未配置，无法处理死信消息: messageId={}, topic={}, tag={}", messageId, topic, tag);
                     }
                     
                     // 确认消息已处理
                     channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                     log.debug("RabbitMQ死信队列消息处理完成: topic={}, tag={}, message={}", topic, tag, finalMessage);
                } catch (Exception e) {
                    log.error("RabbitMQ死信队列消息处理失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
                    // 拒绝消息，避免无限循环
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
                }
            }
        });
        
        // 设置容器启动超时时间
        container.setStartConsumerMinInterval(1000);
        container.setConsecutiveActiveTrigger(1);
        container.setConsecutiveIdleTrigger(1);
        
        container.start();
        
        return container;
    }
    
    /**
     * 内部取消订阅方法，不加锁，供同步块内部调用
     */
    private void unsubscribeInternal(String key) {
        // 停止并移除监听容器
        SimpleMessageListenerContainer container = containerMap.remove(key);
        if (container != null) {
            container.stop();
            container.destroy();
        }
        
        handlerMap.remove(key);
    }

    /**
     * 构建广播队列名称
     */
    private String buildBroadcastQueueName(String topic, String tag) {
        String baseQueueName = topic + ".broadcast";
        if (tag != null && !tag.isEmpty()) {
            baseQueueName += "." + tag;
        }
        // 添加UUID确保每个消费者有独立的队列
        return baseQueueName + "." + java.util.UUID.randomUUID().toString();
    }

    @Override
    public void start() {
        log.info("RabbitMQ消费者启动");
        // 容器在订阅时已经启动，这里可以做一些初始化工作
    }

    @Override
    public void stop() {
        // 使用synchronized确保多线程环境下的停止操作原子性
        synchronized (this) {
            log.info("RabbitMQ消费者停止");
            
            // 停止单播容器
            for (Map.Entry<String, SimpleMessageListenerContainer> entry : containerMap.entrySet()) {
                try {
                    entry.getValue().stop();
                    entry.getValue().destroy();
                    log.info("RabbitMQ单播消费者停止成功: topic={}", entry.getKey());
                } catch (Exception e) {
                    log.warn("停止RabbitMQ单播监听容器失败: topic={}, error={}", entry.getKey(), e.getMessage());
                }
            }
            
            // 停止广播容器
            for (Map.Entry<String, SimpleMessageListenerContainer> entry : broadcastContainerMap.entrySet()) {
                try {
                    entry.getValue().stop();
                    entry.getValue().destroy();
                    log.info("RabbitMQ广播消费者停止成功: consumerKey={}", entry.getKey());
                } catch (Exception e) {
                    log.warn("停止RabbitMQ广播监听容器失败: consumerKey={}, error={}", entry.getKey(), e.getMessage());
                }
            }
            
            containerMap.clear();
            handlerMap.clear();
            broadcastContainerMap.clear();
            broadcastHandlerMap.clear();
        }
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
        // 使用synchronized确保多线程环境下的销毁操作原子性
        synchronized (this) {
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
}