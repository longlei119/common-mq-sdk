package com.example.mq.consumer.impl;

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
import java.util.concurrent.ConcurrentHashMap;
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
     * 消费者是否已启动
     */
    private volatile boolean started = false;

    private final DefaultMQPushConsumer consumer;

    /**
     * 最大重试次数
     */
    private static final int MAX_RETRY_TIMES = 1;

    /**
     * 重试间隔（毫秒）
     */
    private static final long RETRY_INTERVAL_MS = 1000;

    public RocketMQConsumer(DefaultMQPushConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        String key = buildKey(topic, tag);
        handlerMap.put(key, handler);
        log.info("RocketMQ订阅消息：topic={}, tag={}", topic, tag);
        
        try {
            consumer.subscribe(topic, tag);
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                for (MessageExt msg : msgs) {
                    String messageKey = buildKey(msg.getTopic(), msg.getTags());
                    Consumer<String> messageHandler = handlerMap.get(messageKey);
                    if (messageHandler != null) {
                        try {
                            String messageBody = new String(msg.getBody());
                            messageHandler.accept(messageBody);
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        } catch (Exception e) {
                            log.error("处理消息失败：topic={}, tag={}, msgId={}", 
                                msg.getTopic(), msg.getTags(), msg.getMsgId(), e);
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            
            if (!started) {
                start();
            }
        } catch (Exception e) {
            log.error("订阅RocketMQ消息失败：topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("订阅RocketMQ消息失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.ROCKET_MQ) {
            return;
        }
        String key = buildKey(topic, tag);
        handlerMap.remove(key);
        log.info("RocketMQ取消订阅：topic={}, tag={}", topic, tag);
        try {
            consumer.unsubscribe(topic);
        } catch (Exception e) {
            log.error("取消订阅失败：topic={}, tag={}", topic, tag, e);
            throw new RuntimeException("取消订阅失败", e);
        }
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
                    try {
                        consumer.shutdown();
                    } catch (Exception e) {
                        log.error("停止RocketMQ消费者失败", e);
                    } finally {
                        started = false;
                        handlerMap.clear();
                    }
                }
            }
        }
    }

    /**
     * 构建处理器映射的key
     */
    private String buildKey(String topic, String tag) {
        return topic + ":" + tag;
    }
}