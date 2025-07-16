package com.lachesis.windrangerms.mq.factory;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.ActiveMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.EMQXConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RabbitMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RedisConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RocketMQConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import com.lachesis.windrangerms.mq.producer.impl.ActiveMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.EMQXProducer;
import com.lachesis.windrangerms.mq.producer.impl.RabbitMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.RedisProducer;
import com.lachesis.windrangerms.mq.producer.impl.RocketMQProducer;

import java.util.EnumMap;
import java.util.Map;
import org.springframework.lang.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * 消息队列工厂类
 */
@Slf4j
public class MQFactory {

    private final Map<MQTypeEnum, MQProducer> producerMap = new EnumMap<>(MQTypeEnum.class);
    private final Map<MQTypeEnum, MQConsumer> consumerMap = new EnumMap<>(MQTypeEnum.class);

    public MQFactory(@Nullable RocketMQProducer rocketMQProducer,
                     @Nullable RocketMQConsumer rocketMQConsumer,
                     @Nullable RedisProducer redisProducer,
                     @Nullable RedisConsumer redisConsumer,
                     @Nullable ActiveMQProducer activeMQProducer,
                     @Nullable ActiveMQConsumer activeMQConsumer,
                     @Nullable RabbitMQProducer rabbitMQProducer,
                     @Nullable RabbitMQConsumer rabbitMQConsumer,
                     @Nullable EMQXProducer emqxProducer,
                     @Nullable EMQXConsumer emqxConsumer) {
        
        log.info("MQFactory初始化开始");
        log.info("RabbitMQProducer注入状态: {}", rabbitMQProducer != null ? "已注入" : "未注入");
        log.info("RabbitMQConsumer注入状态: {}", rabbitMQConsumer != null ? "已注入" : "未注入");
        
        // 注册RocketMQ生产者和消费者
        if (rocketMQProducer != null) {
            producerMap.put(MQTypeEnum.ROCKET_MQ, rocketMQProducer);
            log.info("注册RocketMQProducer: {}", MQTypeEnum.ROCKET_MQ);
        }
        if (rocketMQConsumer != null) {
            consumerMap.put(MQTypeEnum.ROCKET_MQ, rocketMQConsumer);
            log.info("注册RocketMQConsumer: {}", MQTypeEnum.ROCKET_MQ);
        }
        
        // 注册Redis生产者和消费者
        if (redisProducer != null) {
            producerMap.put(MQTypeEnum.REDIS, redisProducer);
            log.info("注册RedisProducer: {}", MQTypeEnum.REDIS);
        }
        if (redisConsumer != null) {
            consumerMap.put(MQTypeEnum.REDIS, redisConsumer);
            log.info("注册RedisConsumer: {}", MQTypeEnum.REDIS);
        }
        
        // 注册ActiveMQ生产者和消费者
        if (activeMQProducer != null) {
            producerMap.put(MQTypeEnum.ACTIVE_MQ, activeMQProducer);
            log.info("注册ActiveMQProducer: {}", MQTypeEnum.ACTIVE_MQ);
        }
        if (activeMQConsumer != null) {
            consumerMap.put(MQTypeEnum.ACTIVE_MQ, activeMQConsumer);
            log.info("注册ActiveMQConsumer: {}", MQTypeEnum.ACTIVE_MQ);
        }
        
        // 注册RabbitMQ生产者和消费者
        if (rabbitMQProducer != null) {
            producerMap.put(MQTypeEnum.RABBIT_MQ, rabbitMQProducer);
            log.info("注册RabbitMQProducer: {}", MQTypeEnum.RABBIT_MQ);
        }
        if (rabbitMQConsumer != null) {
            consumerMap.put(MQTypeEnum.RABBIT_MQ, rabbitMQConsumer);
            log.info("注册RabbitMQConsumer: {}", MQTypeEnum.RABBIT_MQ);
        }
        
        // 注册EMQX生产者和消费者
        if (emqxProducer != null) {
            producerMap.put(MQTypeEnum.EMQX, emqxProducer);
            log.info("注册EMQXProducer: {}", MQTypeEnum.EMQX);
        }
        if (emqxConsumer != null) {
            consumerMap.put(MQTypeEnum.EMQX, emqxConsumer);
            log.info("注册EMQXConsumer: {}", MQTypeEnum.EMQX);
        }
        
        log.info("MQFactory初始化完成，已注册生产者: {}, 已注册消费者: {}", producerMap.keySet(), consumerMap.keySet());
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
