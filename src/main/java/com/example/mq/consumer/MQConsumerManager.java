package com.example.mq.consumer;

import com.example.mq.consumer.MQConsumer;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.enums.MessageMode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * MQ消费者管理器
 * 统一管理不同MQ类型的消费者注册
 */
@Slf4j
@Component
public class MQConsumerManager {
    
    private final Map<MQTypeEnum, MQConsumer> consumerMap = new ConcurrentHashMap<>();
    
    @Autowired
    public MQConsumerManager(Map<String, MQConsumer> consumers) {
        // 自动注册所有MQ消费者实现
        consumers.forEach((beanName, consumer) -> {
            MQTypeEnum mqType = determineMQType(beanName, consumer);
            if (mqType != null) {
                consumerMap.put(mqType, consumer);
                log.info("注册MQ消费者: type={}, implementation={}", mqType, consumer.getClass().getSimpleName());
            }
        });
    }
    
    /**
     * 订阅单播消息
     */
    public void subscribeUnicast(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler, String consumerGroup) {
        MQConsumer consumer = getConsumer(mqType);
        consumer.subscribeUnicast(topic, tag, handler, consumerGroup);
        log.info("订阅单播消息: mqType={}, topic={}, tag={}, consumerGroup={}", mqType, topic, tag, consumerGroup);
    }
    
    /**
     * 订阅广播消息
     */
    public void subscribeBroadcast(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        MQConsumer consumer = getConsumer(mqType);
        consumer.subscribeBroadcast(topic, tag, handler);
        log.info("订阅广播消息: mqType={}, topic={}, tag={}", mqType, topic, tag);
    }
    
    /**
     * 取消订阅
     */
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        MQConsumer consumer = getConsumer(mqType);
        consumer.unsubscribe(mqType, topic, tag);
        log.info("取消订阅: mqType={}, topic={}, tag={}", mqType, topic, tag);
    }
    
    /**
     * 取消广播订阅
     */
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        MQConsumer consumer = getConsumer(mqType);
        consumer.unsubscribeBroadcast(mqType, topic, tag);
        log.info("取消广播订阅: mqType={}, topic={}, tag={}", mqType, topic, tag);
    }
    
    /**
     * 启动所有消费者
     */
    public void startAll() {
        consumerMap.values().forEach(MQConsumer::start);
        log.info("启动所有MQ消费者");
    }
    
    /**
     * 停止所有消费者
     */
    public void stopAll() {
        consumerMap.values().forEach(MQConsumer::stop);
        log.info("停止所有MQ消费者");
    }
    
    private MQConsumer getConsumer(MQTypeEnum mqType) {
        MQConsumer consumer = consumerMap.get(mqType);
        if (consumer == null) {
            throw new IllegalArgumentException("不支持的MQ类型: " + mqType);
        }
        return consumer;
    }
    
    /**
     * 根据Bean名称和实现类确定MQ类型
     */
    private MQTypeEnum determineMQType(String beanName, MQConsumer consumer) {
        // 直接使用消费者的getMQType方法
        return consumer.getMQType();
    }
}