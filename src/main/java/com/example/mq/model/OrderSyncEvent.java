package com.example.mq.model;

import com.alibaba.fastjson.JSONObject;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 订单同步事件
 */
@Data
@Builder
@EqualsAndHashCode(callSuper = true)
public class OrderSyncEvent extends MQEvent {

    /**
     * 事件类型
     */
    private String eventType;

    /**
     * 订单ID
     */
    private String orderId;

    /**
     * 订单号
     */
    private String orderNum;

    /**
     * 消息体
     */
    private JSONObject data;

    @Override
    public String getTopic() {
        return "mall";
    }

    @Override
    public String getTag() {
        return "order_sync";
    }
}