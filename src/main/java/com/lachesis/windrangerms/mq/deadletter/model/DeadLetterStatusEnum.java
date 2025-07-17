package com.lachesis.windrangerms.mq.deadletter.model;

/**
 * 死信消息状态枚举
 */
public enum DeadLetterStatusEnum {

    /**
     * 待处理 - 消息已进入死信队列，等待处理
     */
    PENDING(0),
    
    /**
     * 已重新投递 - 消息已被重新投递到原队列
     */
    REDELIVERED(1),
    
    /**
     * 投递成功 - 消息重新投递成功
     */
    DELIVERED(2),
    
    /**
     * 投递失败 - 消息重新投递失败
     */
    FAILED(3);

    private final int code;

    DeadLetterStatusEnum(int code) {
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
    public static DeadLetterStatusEnum fromCode(int code) {
        for (DeadLetterStatusEnum status : DeadLetterStatusEnum.values()) {
            if (status.getCode() == code) {
                return status;
            }
        }
        
        return null;
    }
}