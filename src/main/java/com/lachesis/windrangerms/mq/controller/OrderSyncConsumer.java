package com.lachesis.windrangerms.mq.controller;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.OrderSyncEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * 订单同步消费者示例
 */
@Slf4j
@Component
public class OrderSyncConsumer {

    @Autowired(required = false)
    @Qualifier("rocketMQConsumer")
    private MQConsumer mqConsumer;

    @PostConstruct
    public void init() {
        if (mqConsumer == null) {
            log.warn("MQConsumer is not available, order sync consumer will not be initialized");
            return;
        }

        try {
            // 订阅RocketMQ的订单同步消息
            mqConsumer.subscribe(MQTypeEnum.ROCKET_MQ, "mall", "order_sync", message -> {
                try {
                    OrderSyncEvent event = JSON.parseObject(message, OrderSyncEvent.class);
                    log.info("收到订单同步消息：orderId={}, orderNum={}",
                            event.getOrderId(), event.getOrderNum());
                    // 处理订单同步逻辑
                    handleOrderSync(event);
                } catch (Exception e) {
                    log.error("处理订单同步消息失败：message={}", message, e);
                }
            });

            // 启动消费者
            mqConsumer.start();
            log.info("Order sync consumer initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize order sync consumer", e);
        }
    }

    private void handleOrderSync(OrderSyncEvent event) {
        // 实现订单同步的具体业务逻辑
        log.info("订单同步处理完成：orderId={}", event.getOrderId());
    }
}