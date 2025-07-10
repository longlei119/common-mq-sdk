package com.example.mq.delay.adapter;

import com.example.mq.delay.model.DelayMessage;
import com.example.mq.enums.MQTypeEnum;

/**
 * MQ适配器接口
 * 用于统一不同MQ的发送操作
 */
public interface MQAdapter {
    
    /**
     * 发送延迟消息到对应的MQ
     *
     * @param message 延迟消息
     * @return 是否发送成功
     */
    boolean send(DelayMessage message);
    
    /**
     * 获取MQ类型
     *
     * @return MQ类型字符串
     */
    String getMQType();
}