package com.lachesis.windrangerms.mq.deadletter.model;

import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 死信消息实体类
 */
@Data
public class DeadLetterMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * 消息ID
     */
    private String id;
    
    /**
     * 原始消息ID（如果是从延迟队列转移过来的）
     */
    private String originalMessageId;
    
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
     * 消息队列类型
     */
    private String mqType;
    
    /**
     * 失败原因
     */
    private String failureReason;
    
    /**
     * 重试次数
     */
    private int retryCount;
    
    /**
     * 最大重试次数
     */
    private int maxRetryCount;
    
    /**
     * 状态：0-待处理，1-已重新投递，2-投递成功，3-投递失败
     */
    private int status;
    
    /**
     * 创建时间戳
     */
    private long createTimestamp;
    
    /**
     * 更新时间戳
     */
    private long updateTimestamp;

    /**
     * 原始主题
     */
    private String originalTopic;

    /**
     * 原始标签
     */
    private String originalTag;

    /**
     * 原始消息体
     */
    private String originalBody;

    /**
     * 死信时间
     */
    private Long deadLetterTime;

    /**
     * 重试历史
     */
    private List<RetryHistory> retryHistory;

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
    
    /**
     * 获取状态枚举
     * 
     * @return 状态枚举
     */
    public DeadLetterStatusEnum getStatusEnum() {
        return DeadLetterStatusEnum.fromCode(status);
    }

    /**
     * 获取原始主题
     * 
     * @return 原始主题
     */
    public String getOriginalTopic() {
        return originalTopic;
    }

    /**
     * 设置原始主题
     * 
     * @param originalTopic 原始主题
     */
    public void setOriginalTopic(String originalTopic) {
        this.originalTopic = originalTopic;
    }

    /**
     * 获取原始标签
     * 
     * @return 原始标签
     */
    public String getOriginalTag() {
        return originalTag;
    }

    /**
     * 设置原始标签
     * 
     * @param originalTag 原始标签
     */
    public void setOriginalTag(String originalTag) {
        this.originalTag = originalTag;
    }

    /**
     * 获取原始消息体
     * 
     * @return 原始消息体
     */
    public String getOriginalBody() {
        return originalBody;
    }

    /**
     * 设置原始消息体
     * 
     * @param originalBody 原始消息体
     */
    public void setOriginalBody(String originalBody) {
        this.originalBody = originalBody;
    }

    /**
     * 获取死信时间
     * 
     * @return 死信时间
     */
    public Long getDeadLetterTime() {
        return deadLetterTime;
    }

    /**
     * 设置死信时间
     * 
     * @param deadLetterTime 死信时间
     */
    public void setDeadLetterTime(Long deadLetterTime) {
        this.deadLetterTime = deadLetterTime;
    }

    /**
     * 获取重试历史
     * 
     * @return 重试历史
     */
    public List<RetryHistory> getRetryHistory() {
        return retryHistory;
    }

    /**
     * 设置重试历史
     * 
     * @param retryHistory 重试历史
     */
    public void setRetryHistory(List<RetryHistory> retryHistory) {
        this.retryHistory = retryHistory;
    }

    /**
     * 设置状态枚举
     * 
     * @param statusEnum 状态枚举
     */
    public void setStatusEnum(DeadLetterStatusEnum statusEnum) {
        if (statusEnum != null) {
            this.status = statusEnum.getCode();
        }
    }
}