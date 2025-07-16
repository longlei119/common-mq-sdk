// package com.lachesis.windrangerms.mq.example;
//
// import com.lachesis.windrangerms.mq.annotation.MQConsumer;
// import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
// import com.lachesis.windrangerms.mq.enums.MessageMode;
// import lombok.extern.slf4j.Slf4j;
// import org.springframework.stereotype.Service;
//
// /**
//  * 示例消息服务
//  * 演示如何使用@MQConsumer注解来定义消息消费者
//  */
// @Slf4j
// @Service
// public class ExampleMessageService {
//     /**
//      * 处理用户注册单播消息
//      * 只有一个消费者实例会收到消息
//      */
//     @MQConsumer(
//         mqType = MQTypeEnum.REDIS,
//         topic = "user-topic",
//         tag = "register",
//         mode = MessageMode.UNICAST,
//         consumerGroup = "user-service-group"
//     )
//     public void handleUserRegister(String message) {
//         log.info("处理用户注册消息: {}", message);
//         // 处理用户注册逻辑
//     }
//     /**
//      * 处理系统通知广播消息
//      * 所有订阅的消费者实例都会收到消息
//      */
//     @MQConsumer(
//         mqType = MQTypeEnum.REDIS,
//         topic = "system-topic",
//         tag = "notification",
//         mode = MessageMode.BROADCAST,
//         consumerGroup = "notification-service-group"
//     )
//     public void handleSystemNotification(String message) {
//         log.info("处理系统通知广播消息: {}", message);
//         // 处理系统通知逻辑
//     }
//     /**
//      * 处理订单状态更新单播消息
//      */
//     @MQConsumer(
//         mqType = MQTypeEnum.RABBIT_MQ,
//         topic = "order-topic",
//         tag = "status-update",
//         mode = MessageMode.UNICAST,
//         consumerGroup = "order-service-group"
//     )
//     public void handleOrderStatusUpdate(String message) {
//         log.info("处理订单状态更新消息: {}", message);
//         // 处理订单状态更新逻辑
//     }
//     /**
//      * 处理配置变更广播消息
//      * 使用Redis作为MQ
//      */
//     @MQConsumer(
//         mqType = MQTypeEnum.REDIS,
//         topic = "config-topic",
//         tag = "change",
//         mode = MessageMode.BROADCAST,
//         consumerGroup = "config-service-group"
//     )
//     public void handleConfigChange(String message) {
//         log.info("处理配置变更广播消息: {}", message);
//         // 处理配置变更逻辑
//     }
//     /**
//      * 处理日志收集单播消息
//      * 使用Redis（ActiveMQ在测试中被禁用）
//      */
//     @MQConsumer(
//         mqType = MQTypeEnum.REDIS,
//         topic = "log-topic",
//         tag = "collect",
//         mode = MessageMode.UNICAST,
//         consumerGroup = "log-service-group"
//     )
//     public void handleLogCollection(String message) {
//         log.info("处理日志收集消息: {}", message);
//         // 处理日志收集逻辑
//     }
//     /**
//      * 处理IoT设备数据广播消息
//      * 使用RabbitMQ（EMQX在测试中被禁用）
//      */
//     @MQConsumer(
//         mqType = MQTypeEnum.RABBIT_MQ,
//         topic = "iot-topic",
//         tag = "device-data",
//         mode = MessageMode.BROADCAST,
//         consumerGroup = "iot-service-group"
//     )
//     public void handleIoTDeviceData(String message) {
//         log.info("处理IoT设备数据广播消息: {}", message);
//         // 处理IoT设备数据逻辑
//     }
// }