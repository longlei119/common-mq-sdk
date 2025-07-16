package com.lachesis.windrangerms.mq.model;

import lombok.Data;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
     * 消息发送成功时间
     */
    private AtomicLong sendSuccessTime;

    /**
     * 用户自定义属性
     */
    private Map<String, String> userProperties = new HashMap<>();
    
    /**
     * 获取主题
     */
    public abstract String getTopic();
    
    /**
     * 获取标签
     */
    public abstract String getTag();

    /**
     * 设置用户自定义属性
     */
    public void setUserProperty(String key, String value) {
        if (userProperties == null) {
            userProperties = new HashMap<>();
        }
        userProperties.put(key, value);
    }

    /**
     * 获取用户自定义属性
     */
    public String getUserProperty(String key) {
        return userProperties != null ? userProperties.get(key) : null;
    }
}