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

/**
 * 消息队列工厂类
 */
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
        // 注册RocketMQ生产者和消费者
        if (rocketMQProducer != null) {
            producerMap.put(MQTypeEnum.ROCKET_MQ, rocketMQProducer);
        }
        if (rocketMQConsumer != null) {
            consumerMap.put(MQTypeEnum.ROCKET_MQ, rocketMQConsumer);
        }
        
        // 注册Redis生产者和消费者
        if (redisProducer != null) {
            producerMap.put(MQTypeEnum.REDIS, redisProducer);
        }
        if (redisConsumer != null) {
            consumerMap.put(MQTypeEnum.REDIS, redisConsumer);
        }
        
        // 注册ActiveMQ生产者和消费者
        if (activeMQProducer != null) {
            producerMap.put(MQTypeEnum.ACTIVE_MQ, activeMQProducer);
        }
        if (activeMQConsumer != null) {
            consumerMap.put(MQTypeEnum.ACTIVE_MQ, activeMQConsumer);
        }
        
        // 注册RabbitMQ生产者和消费者
        if (rabbitMQProducer != null) {
            producerMap.put(MQTypeEnum.RABBIT_MQ, rabbitMQProducer);
        }
        if (rabbitMQConsumer != null) {
            consumerMap.put(MQTypeEnum.RABBIT_MQ, rabbitMQConsumer);
        }
        
        // 注册EMQX生产者和消费者
        if (emqxProducer != null) {
            producerMap.put(MQTypeEnum.EMQX, emqxProducer);
        }
        if (emqxConsumer != null) {
            consumerMap.put(MQTypeEnum.EMQX, emqxConsumer);
        }
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
