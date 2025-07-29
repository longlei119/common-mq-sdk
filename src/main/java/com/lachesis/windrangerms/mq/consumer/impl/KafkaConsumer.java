package com.lachesis.windrangerms.mq.consumer.impl;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Kafka消费者实现
 */
@Slf4j
@Component
@ConditionalOnBean(org.apache.kafka.clients.consumer.KafkaConsumer.class)
public class KafkaConsumer implements MQConsumer {

    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;
    private final Map<String, Consumer<String>> handlerMap = new ConcurrentHashMap<>();
    private final Map<String, Consumer<String>> broadcastHandlerMap = new ConcurrentHashMap<>();
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private volatile boolean running = false;
    private Future<?> pollingTask;
    
    @Autowired(required = false)
    private DeadLetterServiceFactory deadLetterServiceFactory;

    public KafkaConsumer(org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void subscribeUnicast(String topic, String tag, Consumer<String> handler, String consumerGroup) {
        try {
            String key = buildKey(topic, tag);
            handlerMap.put(key, handler);
            
            // 订阅topic
            synchronized (kafkaConsumer) {
                kafkaConsumer.subscribe(Arrays.asList(topic));
            }
            
            // 启动消费者轮询
            startPolling();
            
            log.info("Kafka单播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("Kafka单播订阅失败: topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("Kafka单播订阅失败", e);
        }
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.KAFKA) {
            return;
        }
        subscribeUnicast(topic, tag, handler, null);
    }

    @Override
    public void subscribeBroadcast(String topic, String tag, Consumer<String> handler) {
        subscribeBroadcast(MQTypeEnum.KAFKA, topic, tag, handler);
    }

    @Override
    public void subscribeBroadcast(MQTypeEnum mqType, String topic, String tag, Consumer<String> messageHandler) {
        try {
            String key = buildBroadcastKey(topic, tag);
            broadcastHandlerMap.put(key, messageHandler);
            
            // 对于广播消息，需要为每个消费者创建独立的消费者组
            // 注意：当前实现使用共享的kafkaConsumer实例，这会导致广播失效
            // 真正的广播需要每个消费者使用不同的消费者组ID
            log.warn("当前Kafka广播实现使用共享消费者组，可能无法实现真正的广播效果");
            
            synchronized (kafkaConsumer) {
                kafkaConsumer.subscribe(Arrays.asList(topic));
            }
            
            // 启动消费者轮询
            startPolling();
            
            log.info("Kafka广播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("Kafka广播订阅失败: topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("Kafka广播订阅失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.KAFKA) {
            return;
        }
        try {
            // 移除相关的处理器
            String key = buildKey(topic, tag);
            handlerMap.remove(key);
            
            String broadcastKey = buildBroadcastKey(topic, tag);
            broadcastHandlerMap.remove(broadcastKey);
            
            // 如果没有其他订阅，则取消订阅
            if (handlerMap.isEmpty() && broadcastHandlerMap.isEmpty()) {
                synchronized (kafkaConsumer) {
                    kafkaConsumer.unsubscribe();
                }
                stopPolling();
            }
            
            log.info("Kafka取消订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("Kafka取消订阅失败: topic={}, tag={}", topic, tag, e);
        }
    }

    @Override
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.KAFKA) {
            return;
        }
        try {
            String broadcastKey = buildBroadcastKey(topic, tag);
            broadcastHandlerMap.remove(broadcastKey);
            
            // 如果没有其他订阅，则取消订阅
            if (handlerMap.isEmpty() && broadcastHandlerMap.isEmpty()) {
                synchronized (kafkaConsumer) {
                    kafkaConsumer.unsubscribe();
                }
                stopPolling();
            }
            
            log.info("Kafka取消广播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("Kafka取消广播订阅失败: topic={}, tag={}", topic, tag, e);
        }
    }

    public void unsubscribe(String topic) {
        try {
            // 移除相关的处理器
            handlerMap.entrySet().removeIf(entry -> entry.getKey().startsWith(topic + ":"));
            broadcastHandlerMap.entrySet().removeIf(entry -> entry.getKey().startsWith(topic + ":"));
            
            // 如果没有其他订阅，则取消订阅
            if (handlerMap.isEmpty() && broadcastHandlerMap.isEmpty()) {
                synchronized (kafkaConsumer) {
                    kafkaConsumer.unsubscribe();
                }
                stopPolling();
            }
            
            log.info("Kafka取消订阅成功: topic={}", topic);
        } catch (Exception e) {
            log.error("Kafka取消订阅失败: topic={}", topic, e);
        }
    }

    @Override
    public void start() {
        log.info("Kafka消费者启动");
        startPolling();
    }

    @Override
    public void stop() {
        log.info("Kafka消费者停止");
        stopPolling();
        // 不在这里关闭线程池，因为可能会有多个测试用例重复使用同一个消费者实例
        // executorService.shutdown();
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.KAFKA;
    }

    /**
     * 启动消息轮询
     */
    private synchronized void startPolling() {
        if (!running) {
            // 如果线程池已关闭，重新创建
            if (executorService.isShutdown()) {
                executorService = Executors.newCachedThreadPool();
                log.info("重新创建线程池");
            }
            running = true;
            pollingTask = executorService.submit(this::pollMessages);
            log.info("Kafka消息轮询已启动");
        }
    }

    /**
     * 停止消息轮询
     */
    private synchronized void stopPolling() {
        if (running) {
            running = false;
            if (pollingTask != null) {
                pollingTask.cancel(true);
            }
            log.info("Kafka消息轮询已停止");
        }
    }

    /**
     * 轮询消息
     */
    private void pollMessages() {
        while (running) {
            try {
                ConsumerRecords<String, String> records;
                synchronized (kafkaConsumer) {
                    records = kafkaConsumer.poll(Duration.ofMillis(1000));
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    processMessage(record);
                }
                
                // 手动提交偏移量
                synchronized (kafkaConsumer) {
                    kafkaConsumer.commitSync();
                }
                
            } catch (Exception e) {
                if (running) {
                    log.error("Kafka消息轮询异常", e);
                    // 发生异常时短暂休眠，避免频繁重试
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }

    /**
     * 处理消息
     */
    private void processMessage(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String tag = record.key(); // 使用key作为tag
        String message = record.value();
        
        log.info("Kafka收到消息: topic={}, tag={}, partition={}, offset={}, message={}", 
                topic, tag, record.partition(), record.offset(), message);
        
        // 尝试单播处理器
        String unicastKey = buildKey(topic, tag);
        Consumer<String> unicastHandler = handlerMap.get(unicastKey);
        
        // 尝试广播处理器
        String broadcastKey = buildBroadcastKey(topic, tag);
        Consumer<String> broadcastHandler = broadcastHandlerMap.get(broadcastKey);
        
        boolean processed = false;
        
        if (unicastHandler != null) {
            processed = processMessageWithRetry(unicastHandler, topic, tag, message, "单播");
        }
        
        if (broadcastHandler != null) {
            boolean broadcastProcessed = processMessageWithRetry(broadcastHandler, topic, tag, message, "广播");
            processed = processed || broadcastProcessed;
        }
        
        if (!processed) {
            log.warn("Kafka消息没有找到对应的处理器: topic={}, tag={}", topic, tag);
        }
    }
    
    /**
     * 带重试机制的消息处理
     */
    private boolean processMessageWithRetry(Consumer<String> handler, String topic, String tag, String message, String handlerType) {
        int maxRetries = 3;
        int retryCount = 0;
        Exception lastException = null;
        
        while (retryCount < maxRetries) {
            retryCount++;
            try {
                handler.accept(message);
                log.info("Kafka{}消息处理成功: topic={}, tag={}, 尝试次数={}", handlerType, topic, tag, retryCount);
                return true;
            } catch (Exception e) {
                lastException = e;
                log.error("Kafka{}消息处理失败: topic={}, tag={}, message={}, 尝试次数={}/{}", 
                         handlerType, topic, tag, message, retryCount, maxRetries, e);
                
                if (retryCount < maxRetries) {
                    // 重试前等待一段时间
                    try {
                        Thread.sleep(1000 * retryCount); // 递增等待时间
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        // 所有重试都失败后，发送到死信队列
        log.error("Kafka{}消息处理最终失败，发送到死信队列: topic={}, tag={}", handlerType, topic, tag);
        handleMessageFailure(topic, tag, message, lastException);
        return false;
    }

    /**
     * 处理消息失败
     */
    private void handleMessageFailure(String topic, String tag, String message, Exception e) {
        try {
            if (deadLetterServiceFactory != null) {
                DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
                if (deadLetterService != null) {
                    // 创建死信消息对象
                    com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage deadLetterMessage = new com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage();
                    deadLetterMessage.setId(java.util.UUID.randomUUID().toString().replace("-", ""));
                    deadLetterMessage.setOriginalMessageId(java.util.UUID.randomUUID().toString().replace("-", ""));
                    deadLetterMessage.setTopic(topic);
                    deadLetterMessage.setTag(tag);
                    deadLetterMessage.setBody(message);
                    deadLetterMessage.setMqType(MQTypeEnum.KAFKA.name());
                    deadLetterMessage.setFailureReason("Kafka消息处理失败: " + e.getMessage());
                    deadLetterMessage.setRetryCount(0);
                    deadLetterMessage.setMaxRetryCount(3);
                    deadLetterMessage.setCreateTimestamp(System.currentTimeMillis());
                    deadLetterMessage.setUpdateTimestamp(System.currentTimeMillis());
                    
                    // 保存到死信队列服务中
                    boolean saved = deadLetterService.saveDeadLetterMessage(deadLetterMessage);
                    if (saved) {
                        log.info("Kafka消息已发送到死信队列: topic={}, tag={}", topic, tag);
                    } else {
                        log.error("Kafka消息发送到死信队列失败: topic={}, tag={}", topic, tag);
                    }
                }
            }
        } catch (Exception ex) {
            log.error("发送Kafka消息到死信队列失败: topic={}, tag={}, message={}", topic, tag, message, ex);
        }
    }

    /**
     * 构建处理器映射的key
     */
    private String buildKey(String topic, String tag) {
        return topic + ":" + (tag != null ? tag : "");
    }

    /**
     * 构建广播处理器映射的key
     */
    private String buildBroadcastKey(String topic, String tag) {
        return "broadcast:" + topic + ":" + (tag != null ? tag : "");
    }

    /**
     * 关闭消费者
     */
    public void close() {
        stop();
        // 关闭线程池
        if (!executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (kafkaConsumer != null) {
            synchronized (kafkaConsumer) {
                kafkaConsumer.close();
            }
            log.info("Kafka消费者已关闭");
        }
    }
}