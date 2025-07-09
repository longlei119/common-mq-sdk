package com.example.mq.model;

import lombok.Data;

/**
 * MQ事件基类
 */
@Data
public abstract class MQEvent {
    
    /**
     * 消息ID
     */
    private String messageId;
    
    /**
     * 租户ID
     */
    private String tenantId;
    
    /**
     * 获取主题
     */
    public abstract String getTopic();
    
    /**
     * 获取标签
     */
    public abstract String getTag();
}