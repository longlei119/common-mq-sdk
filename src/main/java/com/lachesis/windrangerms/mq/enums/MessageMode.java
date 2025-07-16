package com.lachesis.windrangerms.mq.enums;

/**
 * 消息模式枚举
 */
public enum MessageMode {
    
    /**
     * 单播模式（点对点）
     * 消息只会被一个消费者接收
     */
    UNICAST,
    
    /**
     * 广播模式（发布订阅）
     * 消息会被所有订阅的消费者接收
     */
    BROADCAST
}