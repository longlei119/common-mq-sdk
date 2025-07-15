package com.example.mq.delay.model;

/**
 * 消息状态枚举
 * 定义延迟消息的状态常量
 */
public enum MessageStatusEnum {

    /**
     * 等待中 - 消息已保存，等待投递
     */
    WAITING(0),
    
    /**
     * 投递中 - 消息正在投递到目标队列
     */
    DELIVERING(1),
    
    /**
     * 投递成功 - 消息已成功投递到目标队列
     */
    DELIVERED(2),
    
    /**
     * 投递失败 - 消息投递失败，等待重试
     */
    FAILED(3),
    
    /**
     * 已过期 - 消息已过期，不再投递
     */
    EXPIRED(4),
    
    /**
     * 已取消 - 消息被手动取消
     */
    CANCELED(5);

    private final int code;

    MessageStatusEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /**
     * 根据状态码获取枚举值
     * 
     * @param code 状态码
     * @return 枚举值，如果不存在则返回null
     */
    public static MessageStatusEnum fromCode(int code) {
        for (MessageStatusEnum status : MessageStatusEnum.values()) {
            if (status.getCode() == code) {
                return status;
            }
        }
        
        return null;
    }
}