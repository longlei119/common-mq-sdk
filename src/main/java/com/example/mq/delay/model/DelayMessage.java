package com.example.mq.delay.model;

import com.example.mq.delay.model.MessageStatusEnum;
import com.example.mq.enums.MQTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * 延迟消息实体类
 */
@Data
public class DelayMessage implements Serializable {
    /**
     * 消息ID
     */
    private String id;
    
    /**
     * 主题
     */
    private String topic;
    
    /**
     * 标签（可选）
     */
    private String tag;
    
    /**
     * 消息体
     */
    private String body;
    
    /**
     * 属性
     */
    private Map<String, String> properties;
    
    /**
     * 投递时间戳（毫秒）
     */
    private long deliverTimestamp;
    
    /**
     * 消息队列类型
     */
    private String mqType;
    
    /**
     * 重试次数
     */
    private int retryCount;
    
    /**
     * 消息状态
     */
    private int status = MessageStatusEnum.WAITING.getCode();
    
    /**
     * 创建时间戳
     */
    private long createTimestamp = System.currentTimeMillis();
    
    /**
     * 获取消息状态枚举
     * 
     * @return 消息状态枚举
     */
    public MessageStatusEnum getStatusEnum() {
        return MessageStatusEnum.fromCode(status);
    }
    
    /**
     * 设置消息状态枚举
     * 
     * @param statusEnum 消息状态枚举
     */
    public void setStatusEnum(MessageStatusEnum statusEnum) {
        if (statusEnum != null) {
            this.status = statusEnum.getCode();
        }
    }
    
    /**
     * 获取消息队列类型枚举
     * 
     * @return 消息队列类型枚举
     */
    public MQTypeEnum getMqTypeEnum() {
        return MQTypeEnum.fromString(mqType);
    }
    
    /**
     * 设置消息队列类型枚举
     * 
     * @param mqTypeEnum 消息队列类型枚举
     */
    public void setMqTypeEnum(MQTypeEnum mqTypeEnum) {
        if (mqTypeEnum != null) {
            this.mqType = mqTypeEnum.getType();
        }
    }
}