package com.example.mq.annotation;

import com.example.mq.enums.MQTypeEnum;
import com.example.mq.enums.MessageMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * MQ消费者注解
 * 用于标识消息消费方法及其配置
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MQConsumer {
    
    /**
     * MQ类型
     */
    MQTypeEnum mqType();
    
    /**
     * 主题名称
     */
    String topic();
    
    /**
     * 标签（可选）
     */
    String tag() default "";
    
    /**
     * 消息模式：单播或广播
     * 默认为单播模式
     */
    MessageMode mode() default MessageMode.UNICAST;
    
    /**
     * 消费者组名
     */
    String consumerGroup() default "";
}