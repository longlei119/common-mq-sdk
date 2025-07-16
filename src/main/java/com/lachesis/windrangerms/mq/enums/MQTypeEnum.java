package com.lachesis.windrangerms.mq.enums;

/**
 * MQ类型枚举
 */
public enum MQTypeEnum {
    
    /**
     * Redis消息队列
     */
    REDIS("REDIS"),
    
    /**
     * RocketMQ消息队列
     */
    ROCKET_MQ("ROCKET_MQ"),
    
    /**
     * Kafka消息队列
     */
    KAFKA("KAFKA"),
    
    /**
     * RabbitMQ消息队列
     */
    RABBIT_MQ("RABBIT_MQ"),

    /**
     * EMQX消息队列
     */
    EMQX("EMQX"),

    /**
     * ActiveMQ消息队列
     */
    ACTIVE_MQ("ACTIVE_MQ");

    private final String type;

    MQTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * 根据类型字符串获取枚举值
     * 
     * @param type 类型字符串
     * @return 枚举值，如果不存在则返回null
     */
    public static MQTypeEnum fromString(String type) {
        if (type == null) {
            return null;
        }
        
        for (MQTypeEnum mqType : MQTypeEnum.values()) {
            if (mqType.getType().equalsIgnoreCase(type)) {
                return mqType;
            }
        }
        
        return null;
    }
}