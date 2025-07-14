package com.example.mq.consumer.impl;

import com.example.mq.config.MQConfig;
import com.example.mq.consumer.MQConsumer;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * RocketMQ消费者实现
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
public class RocketMQConsumer implements MQConsumer {

    /**
     * 消息处理器映射，key为topic:tag
     */
    private final Map<String, Consumer<String>> handlerMap = new ConcurrentHashMap<>();

    /**
     * 广播消息处理器映射，key为topic:tag:consumerGroup
     */
    private final Map<String, Consumer<String>> broadcastHandlerMap = new ConcurrentHashMap<>();

    /**
     * 广播消费者映射，key为topic:tag:consumerGroup
     */
    private final Map<String, DefaultMQPushConsumer> broadcastConsumerMap = new ConcurrentHashMap<>();

    /**
     * 消费者是否已启动
     */
    private volatile boolean started = false;

    private final DefaultMQPushConsumer consumer;
    private final MQConfig.RocketMQProperties rocketMQProperties;

    /**
     * 最大重试次数
     */
    private static final int MAX_RETRY_TIMES = 1;

    /**
     * 重试间隔（毫秒）
     */
    private static final long RETRY_INTERVAL_MS = 1000;

    public RocketMQConsumer(DefaultMQPushConsumer consumer, MQConfig.RocketMQProperties rocketMQProperties) {
        this.consumer = consumer;
        this.rocketMQProperties = rocketMQProperties;

        // 配置消费者参数
        consumer.setConsumeThreadMin(rocketMQProperties.getConsumer().getThreadMin());
        consumer.setConsumeThreadMax(rocketMQProperties.getConsumer().getThreadMax());
        consumer.setConsumeMessageBatchMaxSize(rocketMQProperties.getConsumer().getBatchMaxSize());

        // 在构造函数中注册全局消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                String messageKey = buildKey(msg.getTopic(), msg.getTags());
                Consumer<String> messageHandler = handlerMap.get(messageKey);
                log.info("RocketMQ收到消息：topic={}, tag={}, key={}, 是否有处理器={}", 
                        msg.getTopic(), msg.getTags(), messageKey, messageHandler != null);
                
                if (messageHandler != null) {
                    try {
                        String messageBody = new String(msg.getBody());
                        String receiveTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date());
                        log.info("RocketMQ接收到消息：topic={}, tag={}, msgId={}, receiveTime={}, 消费者组={}", 
                                msg.getTopic(), msg.getTags(), msg.getMsgId(), receiveTime, consumer.getConsumerGroup());
                        
                        // 直接调用处理器，不使用CompletableFuture
                        messageHandler.accept(messageBody);
                        log.info("RocketMQ消息处理完成：topic={}, tag={}, msgId={}", msg.getTopic(), msg.getTags(), msg.getMsgId());
                    } catch (Exception e) {
                        log.error("处理RocketMQ消息失败：topic={}, tag={}, msgId={}", 
                            msg.getTopic(), msg.getTags(), msg.getMsgId(), e);
                        
                        // 检查重试次数
                        if (msg.getReconsumeTimes() >= MAX_RETRY_TIMES) {
                            log.error("消息处理失败且超过最大重试次数，将被跳过：topic={}, tag={}, msgId={}, retryTimes={}", 
                                msg.getTopic(), msg.getTags(), msg.getMsgId(), msg.getReconsumeTimes(), e);
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 不再重试
                        }
                        
                        log.warn("处理消息失败，将进行重试：topic={}, tag={}, msgId={}, retryTimes={}", 
                            msg.getTopic(), msg.getTags(), msg.getMsgId(), msg.getReconsumeTimes(), e);
                        // 只对当前失败的消息进行重试，而不是整个批次
                        context.setDelayLevelWhenNextConsume(1); // 1秒后重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                } else {
                    log.warn("RocketMQ收到消息但没有找到对应的处理器：topic={}, tag={}, msgId={}, 当前注册的处理器：{}", 
                            msg.getTopic(), msg.getTags(), msg.getMsgId(), handlerMap.keySet());
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            throw new IllegalArgumentException("MQType must be ROCKET_MQ");
        }
        
        try {
            // 确保消费者已启动
            if (!started) {
                start();
            }
            
            log.info("RocketMQ单播订阅开始: topic={}, tag={}", topic, tag);
            // 只订阅单播消息，广播消息需要通过subscribeBroadcast方法单独订阅
            subscribeUnicastInternal(topic, tag, handler);
            
            log.info("RocketMQ单播订阅成功: topic={}, tag={}, 消费者组={}", topic, tag, rocketMQProperties.getConsumer().getGroupName());
        } catch (Exception e) {
            log.error("RocketMQ订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("RocketMQ订阅失败", e);
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
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        subscribeBroadcastInternal(topic, tag, handler);
    }
    
    /**
     * 单播订阅的内部实现
     */
    private void subscribeUnicastInternal(String topic, String tag, Consumer<String> handler) {
        String key = buildKey(topic, tag);
        
        try {
            // 确保消费者已启动
            if (!started) {
                start();
            }
            
            // 检查是否已经订阅过这个topic和tag
            if (handlerMap.containsKey(key)) {
                log.warn("重复订阅RocketMQ单播消息：topic={}, tag={}, 将更新处理器", topic, tag);
                // 更新处理器
                handlerMap.put(key, handler);
                log.info("RocketMQ单播订阅处理器更新成功：topic={}, tag={}", topic, tag);
                // 重复订阅时也需要重新调用consumer.subscribe()以确保订阅生效
                consumer.subscribe(topic, tag);
                log.info("RocketMQ重新订阅成功：topic={}, tag={}", topic, tag);
                return;
            }
            
            // 先注册处理器
            handlerMap.put(key, handler);
            log.debug("RocketMQ单播订阅：topic={}, tag={}", topic, tag);
            
            // 订阅主题
            consumer.subscribe(topic, tag);
            
            // 确保消费者已启动并订阅成功
            log.info("RocketMQ单播订阅成功：topic={}, tag={}, 消费者组={}", topic, tag, consumer.getConsumerGroup());
            
            // 打印当前注册的所有处理器
            log.info("当前注册的处理器：{}", handlerMap.keySet());
        } catch (Exception e) {
            // 发生异常时，移除处理器
            handlerMap.remove(key);
            log.error("订阅RocketMQ单播消息失败：topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("订阅RocketMQ单播消息失败", e);
        }
    }

    /**
     * 广播订阅的内部实现
     */
    private void subscribeBroadcastInternal(String topic, String tag, Consumer<String> handler) {
        MQTypeEnum mqType = MQTypeEnum.ROCKET_MQ;
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        
        // 广播模式使用独立的消费者组，添加UUID确保每个消费者独立
        // 注意：RocketMQ只允许消费者组名包含字母、数字、下划线、连字符和百分号
        String uuid = java.util.UUID.randomUUID().toString().replace("-", "_");
        String consumerGroup = rocketMQProperties.getConsumer().getGroupName() + "_broadcast_" + uuid;
        String key = buildBroadcastKey(topic, tag, consumerGroup);
        
        try {
            // 创建独立的广播消费者
            DefaultMQPushConsumer broadcastConsumer = new DefaultMQPushConsumer();
            broadcastConsumer.setNamesrvAddr(rocketMQProperties.getNameServerAddr());
            broadcastConsumer.setConsumerGroup(consumerGroup);
            broadcastConsumer.setConsumeFromWhere(rocketMQProperties.getConsumer().getConsumeFromWhere());
            
            // 设置为广播模式
            broadcastConsumer.setMessageModel(org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING);
            
            // 配置消费者参数
            broadcastConsumer.setConsumeThreadMin(rocketMQProperties.getConsumer().getThreadMin());
            broadcastConsumer.setConsumeThreadMax(rocketMQProperties.getConsumer().getThreadMax());
            broadcastConsumer.setConsumeMessageBatchMaxSize(rocketMQProperties.getConsumer().getBatchMaxSize());
            
            // 注册消息监听器
            broadcastConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                for (MessageExt msg : msgs) {
                    try {
                        String messageBody = new String(msg.getBody());
                        String receiveTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date());
                        log.info("RocketMQ接收到广播消息：topic={}, tag={}, msgId={}, consumerGroup={}, receiveTime={}", 
                                msg.getTopic(), msg.getTags(), msg.getMsgId(), consumerGroup, receiveTime);
                        
                        // 使用CompletableFuture实现超时控制
                        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                            handler.accept(messageBody);
                        });
                        
                        try {
                            future.get(rocketMQProperties.getConsumer().getConsumeTimeout(), TimeUnit.MILLISECONDS);
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        } catch (TimeoutException e) {
                            log.error("广播消息处理超时：topic={}, tag={}, msgId={}, timeout={}", 
                                msg.getTopic(), msg.getTags(), msg.getMsgId(), 
                                rocketMQProperties.getConsumer().getConsumeTimeout());
                            future.cancel(true);
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 广播模式不重试
                        } catch (Exception e) {
                            log.error("等待广播消息处理完成时发生错误：topic={}, tag={}, msgId={}", 
                                msg.getTopic(), msg.getTags(), msg.getMsgId(), e);
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 广播模式不重试
                        }
                    } catch (Exception e) {
                        log.error("处理广播消息失败：topic={}, tag={}, msgId={}, consumerGroup={}", 
                            msg.getTopic(), msg.getTags(), msg.getMsgId(), consumerGroup, e);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 广播模式不重试
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            
            // 订阅主题
            broadcastConsumer.subscribe(topic, tag);
            
            // 启动消费者
            broadcastConsumer.start();
            
            // 保存处理器和消费者
            broadcastHandlerMap.put(key, handler);
            broadcastConsumerMap.put(key, broadcastConsumer);
            
            log.info("RocketMQ广播订阅成功：topic={}, tag={}, consumerGroup={}", topic, tag, consumerGroup);
        } catch (Exception e) {
            log.error("订阅RocketMQ广播消息失败：topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("订阅RocketMQ广播消息失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        
        try {
            // 取消单播订阅
            unsubscribeUnicastInternal(topic, tag);
            
            // 取消广播订阅
            unsubscribeBroadcastInternal(topic, tag);
            
            log.info("RocketMQ取消订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("RocketMQ取消订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("RocketMQ取消订阅失败", e);
        }
    }
    
    @Override
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        
        try {
            unsubscribeBroadcastInternal(topic, tag);
            log.info("RocketMQ取消广播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("RocketMQ取消广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("RocketMQ取消广播订阅失败", e);
        }
    }
    
    /**
     * 取消单播订阅的内部实现
     */
    private void unsubscribeUnicastInternal(String topic, String tag) {
        String key = buildKey(topic, tag);
        handlerMap.remove(key);
        log.debug("RocketMQ取消单播订阅：topic={}, tag={}", topic, tag);
        try {
            consumer.unsubscribe(topic);
        } catch (Exception e) {
            log.error("取消单播订阅失败：topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("取消单播订阅失败", e);
        }
    }
    
    /**
     * 取消广播订阅的内部实现
     */
    private void unsubscribeBroadcastInternal(String topic, String tag) {
        String keyPrefix = topic + ":" + tag + ":";
        
        // 移除所有匹配的广播订阅
        broadcastConsumerMap.entrySet().removeIf(entry -> {
            if (entry.getKey().startsWith(keyPrefix)) {
                try {
                    entry.getValue().shutdown();
                    broadcastHandlerMap.remove(entry.getKey());
                    log.debug("RocketMQ取消广播订阅成功：key={}", entry.getKey());
                    return true;
                } catch (Exception e) {
                    log.warn("停止RocketMQ广播消费者失败：key={}, error={}", entry.getKey(), e.getMessage());
                    return false;
                }
            }
            return false;
        });
    }

    @Override
    public void start() {
        if (!started) {
            synchronized (this) {
                if (!started) {
                    int retryCount = 0;
                    Exception lastException = null;

                    while (retryCount < MAX_RETRY_TIMES) {
                        try {
                            log.info("正在启动RocketMQ消费者，第{}次尝试", retryCount + 1);
                            consumer.start();
                            started = true;
                            log.info("RocketMQ消费者启动成功");
                            return;
                        } catch (Exception e) {
                            lastException = e;
                            if (e.getCause() instanceof RemotingConnectException) {
                                log.warn("RocketMQ服务器连接失败（{}:{}），{}秒后重试：{}", 
                                    consumer.getNamesrvAddr(),
                                    RETRY_INTERVAL_MS / 1000, 
                                    e.getMessage());
                                try {
                                    Thread.sleep(RETRY_INTERVAL_MS);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException("启动过程被中断", ie);
                                }
                                retryCount++;
                            } else {
                                log.error("启动RocketMQ消费者失败，发生非连接类异常", e);
                                throw new RuntimeException("启动RocketMQ消费者失败", e);
                            }
                        }
                    }

                    String errorMsg = String.format("RocketMQ消费者启动失败：无法连接到RocketMQ服务器（%s），" +
                        "请检查以下内容：\n" +
                        "1. RocketMQ服务是否已启动\n" +
                        "2. 服务地址配置是否正确\n" +
                        "3. 网络连接是否正常", 
                        consumer.getNamesrvAddr());
                    log.error(errorMsg);
                    throw new RuntimeException(errorMsg, lastException);
                }
            }
        }
    }

    @Override
    public void stop() {
        if (started) {
            synchronized (this) {
                if (started) {
                    log.info("停止RocketMQ消费者");
                    
                    // 停止单播消费者
                    try {
                        consumer.shutdown();
                        log.info("RocketMQ单播消费者停止成功");
                    } catch (Exception e) {
                        log.error("停止RocketMQ单播消费者失败", e);
                    }
                    
                    // 停止所有广播消费者
                    for (Map.Entry<String, DefaultMQPushConsumer> entry : broadcastConsumerMap.entrySet()) {
                        try {
                            entry.getValue().shutdown();
                            log.info("RocketMQ广播消费者停止成功: key={}", entry.getKey());
                        } catch (Exception e) {
                            log.warn("停止RocketMQ广播消费者失败: key={}, error={}", entry.getKey(), e.getMessage());
                        }
                    }
                    
                    // 清理资源
                    started = false;
                    handlerMap.clear();
                    broadcastHandlerMap.clear();
                    broadcastConsumerMap.clear();
                }
            }
        }
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.ROCKET_MQ;
    }

    /**
     * 构建处理器映射的key
     */
    private String buildKey(String topic, String tag) {
        return topic + ":" + tag;
    }

    /**
     * 构建广播处理器映射的key
     */
    private String buildBroadcastKey(String topic, String tag, String consumerGroup) {
        return topic + ":" + tag + ":" + consumerGroup;
    }
}