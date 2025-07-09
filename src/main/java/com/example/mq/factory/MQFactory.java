package com.example.mq.factory;

import com.example.mq.consumer.MQConsumer;
import com.example.mq.consumer.impl.RedisConsumer;
import com.example.mq.consumer.impl.RocketMQConsumer;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.producer.MQProducer;
import com.example.mq.producer.impl.RedisProducer;
import com.example.mq.producer.impl.RocketMQProducer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.EnumMap;
import java.util.Map;
import org.springframework.lang.Nullable;

/**
 * 消息队列工厂类
 */
public class MQFactory {

    private final Map<MQTypeEnum, MQProducer> producerMap = new EnumMap<>(MQTypeEnum.class);
    private final Map<MQTypeEnum, MQConsumer> consumerMap = new EnumMap<>(MQTypeEnum.class);

    public MQFactory(@Nullable RocketMQProducer rocketMQProducer, @Nullable RocketMQConsumer rocketMQConsumer,
                     StringRedisTemplate redisTemplate, RedisMessageListenerContainer redisListenerContainer) {
        // 注册RocketMQ的实现（如果可用）
        if (rocketMQProducer != null) {
            producerMap.put(MQTypeEnum.ROCKET_MQ, rocketMQProducer);
        }
        if (rocketMQConsumer != null) {
            consumerMap.put(MQTypeEnum.ROCKET_MQ, rocketMQConsumer);
        }

        // 注册Redis的实现
        RedisProducer redisProducer = new RedisProducer(redisTemplate);
        RedisConsumer redisConsumer = new RedisConsumer(redisListenerContainer);
        producerMap.put(MQTypeEnum.REDIS, redisProducer);
        consumerMap.put(MQTypeEnum.REDIS, redisConsumer);
    }

    /**
     * 获取指定类型的消息生产者
     *
     * @param mqType 消息队列类型
     * @return 消息生产者实例
     * @throws IllegalArgumentException 如果指定类型的消息队列未配置或不可用
     */
    public MQProducer getProducer(MQTypeEnum mqType) {
        MQProducer producer = producerMap.get(mqType);
        if (producer == null) {
            throw new IllegalArgumentException("MQ type " + mqType + " is not configured or not available");
        }
        return producer;
    }

    /**
     * 获取指定类型的消息消费者
     *
     * @param mqType 消息队列类型
     * @return 消息消费者实例
     * @throws IllegalArgumentException 如果指定类型的消息队列未配置或不可用
     */
    public MQConsumer getConsumer(MQTypeEnum mqType) {
        MQConsumer consumer = consumerMap.get(mqType);
        if (consumer == null) {
            throw new IllegalArgumentException("MQ type " + mqType + " is not configured or not available");
        }
        return consumer;
    }
}