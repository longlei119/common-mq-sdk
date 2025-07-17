package com.lachesis.windrangerms.mq.deadletter.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 重试历史记录模型
 * 记录死信消息的重试历史信息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RetryHistory {
    
    /**
     * 重试历史记录ID
     */
    private String id;
    
    /**
     * 关联的死信消息ID
     */
    private String messageId;
    
    /**
     * 重试时间戳
     */
    private long retryTimestamp;
    
    /**
     * 重试是否成功
     */
    private boolean success;
    
    /**
     * 失败原因（重试失败时记录）
     */
    private String failureReason;
    
    /**
     * 重试次数（第几次重试）
     */
    private int retryCount;
}