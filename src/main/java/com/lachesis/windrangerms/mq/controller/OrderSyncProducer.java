package com.lachesis.windrangerms.mq.controller;

import com.alibaba.fastjson.JSONObject;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.OrderSyncEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 订单同步生产者示例
 */
@Slf4j
@RestController
@RequestMapping("/api/order")
@ConditionalOnBean(name = "rocketMQProducer")
public class OrderSyncProducer {

    @Autowired
    @Qualifier("rocketMQProducer")
    private MQProducer mqProducer;

    @PostMapping("/sync")
    public String syncOrder() {
        // 构建订单信息
        JSONObject orderInfo = new JSONObject();
        orderInfo.put("id", "1001");
        orderInfo.put("name", "测试订单");
        orderInfo.put("amount", 100.00);
        orderInfo.put("status", "CREATED");

        // 创建订单同步事件
        OrderSyncEvent event = OrderSyncEvent.builder()
                .orderId("1001")
                .orderNum("ORDER_" + System.currentTimeMillis())
                .data(orderInfo)
                .eventType("CREATE")
                .build();

        // 设置租户ID
        event.setTenantId("tenant_001");
        // 设置消息ID
        event.setMessageId("msg_" + System.currentTimeMillis());

        try {
            // 同步发送消息到RocketMQ
            mqProducer.syncSend(MQTypeEnum.ROCKET_MQ, event.getTopic(), event.getTag(), event);
            log.info("订单同步消息发送成功：orderId={}", event.getOrderId());
            return "消息发送成功";
        } catch (Exception e) {
            log.error("订单同步消息发送失败：event={}", event, e);
            return "消息发送失败：" + e.getMessage();
        }
    }

    @PostMapping("/async")
    public String asyncOrder() {
        // 构建订单信息
        JSONObject orderInfo = new JSONObject();
        orderInfo.put("id", "1002");
        orderInfo.put("name", "测试订单");
        orderInfo.put("amount", 200.00);
        orderInfo.put("status", "CREATED");

        // 创建订单同步事件
        OrderSyncEvent event = OrderSyncEvent.builder()
                .orderId("1002")
                .orderNum("ORDER_" + System.currentTimeMillis())
                .data(orderInfo)
                .eventType("CREATE")
                .build();

        // 设置租户ID
        event.setTenantId("tenant_001");
        // 设置消息ID
        event.setMessageId("msg_" + System.currentTimeMillis());

        try {
            // 异步发送消息到RocketMQ
            mqProducer.asyncSend(MQTypeEnum.ROCKET_MQ, event.getTopic(), event.getTag(), event);
            log.info("订单同步消息异步发送成功：orderId={}", event.getOrderId());
            return "消息发送成功";
        } catch (Exception e) {
            log.error("订单同步消息发送失败：event={}", event, e);
            return "消息发送失败：" + e.getMessage();
        }
    }
}