package com.example.mq.enums;

/**
 * MQ类型枚举
 */
public enum MQTypeEnum {
    
    /**
     * Redis消息队列
     */
    REDIS,
    
    /**
     * RocketMQ消息队列
     */
    ROCKET_MQ,
    
    /**
     * Kafka消息队列
     */
    KAFKA,
    
    /**
     * RabbitMQ消息队列
     */
    RABBIT_MQ,

    /**
     * EMQX消息队列
     */
    EMQX,

    /**
     * ActiveMQ消息队列
     */
    ACTIVE_MQ
}