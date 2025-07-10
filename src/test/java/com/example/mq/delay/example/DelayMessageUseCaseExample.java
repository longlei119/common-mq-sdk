package com.example.mq.delay.example;

import com.example.mq.delay.model.DelayMessage;
import com.example.mq.delay.DelayMessageSender;
import com.example.mq.enums.MQTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 延迟消息使用场景示例
 * 展示延迟消息在不同业务场景中的应用
 */
@Service
public class DelayMessageUseCaseExample {

    private static final Logger logger = LoggerFactory.getLogger(DelayMessageUseCaseExample.class);

    @Autowired
    private DelayMessageSender delayMessageSender;

    /**
     * 场景一：订单超时自动取消
     * 下单后30分钟内未支付，自动取消订单
     * 
     * @param orderId 订单ID
     */
    public void handleOrderTimeout(String orderId) {
        logger.info("创建订单超时自动取消的延迟消息，订单ID: {}", orderId);
        
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId(UUID.randomUUID().toString());
        message.setTopic("order-topic");
        message.setTag("order-timeout");
        message.setBody(orderId);
        message.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        
        // 设置30分钟后投递
        message.setDeliverTimestamp(System.currentTimeMillis() + 30 * 60 * 1000);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("businessType", "orderTimeout");
        properties.put("orderId", orderId);
        message.setProperties(properties);
        
        // 发送延迟消息
        delayMessageSender.sendDelayMessage(message);
        
        logger.info("订单超时自动取消的延迟消息已发送，订单ID: {}, 消息ID: {}", orderId, message.getId());
    }

    /**
     * 场景二：优惠券到期提醒
     * 优惠券到期前3天发送提醒消息
     * 
     * @param couponId 优惠券ID
     * @param userId 用户ID
     * @param expireTime 过期时间戳
     */
    public void sendCouponExpireReminder(String couponId, String userId, long expireTime) {
        logger.info("创建优惠券到期提醒的延迟消息，优惠券ID: {}, 用户ID: {}", couponId, userId);
        
        // 计算提醒时间（到期前3天）
        long remindTime = expireTime - 3 * 24 * 60 * 60 * 1000;
        long currentTime = System.currentTimeMillis();
        
        // 如果已经过了提醒时间，则不发送
        if (remindTime <= currentTime) {
            logger.info("优惠券即将到期或已过期，不发送提醒，优惠券ID: {}", couponId);
            return;
        }
        
        // 计算延迟时间
        long delayTime = remindTime - currentTime;
        
        // 使用简化方法发送延迟消息
        String messageBody = String.format("{\"couponId\":\"%s\",\"userId\":\"%s\",\"expireTime\":%d}", 
                couponId, userId, expireTime);
        
        String messageId = delayMessageSender.sendDelayMessage(
                "notification-topic",
                "coupon-expire",
                messageBody,
                "ROCKET_MQ",
                delayTime
        );
        
        logger.info("优惠券到期提醒的延迟消息已发送，优惠券ID: {}, 用户ID: {}, 消息ID: {}", 
                couponId, userId, messageId);
    }

    /**
     * 场景三：预约提醒
     * 预约时间前15分钟发送提醒消息
     * 
     * @param appointmentId 预约ID
     * @param userId 用户ID
     * @param appointmentTime 预约时间戳
     * @param appointmentType 预约类型
     */
    public void sendAppointmentReminder(String appointmentId, String userId, 
                                       long appointmentTime, String appointmentType) {
        logger.info("创建预约提醒的延迟消息，预约ID: {}, 用户ID: {}", appointmentId, userId);
        
        // 计算提醒时间（预约前15分钟）
        long remindTime = appointmentTime - 15 * 60 * 1000;
        long currentTime = System.currentTimeMillis();
        
        // 如果已经过了提醒时间，则不发送
        if (remindTime <= currentTime) {
            logger.info("预约时间即将到来或已过期，不发送提醒，预约ID: {}", appointmentId);
            return;
        }
        
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId(UUID.randomUUID().toString());
        message.setTopic("notification-topic");
        message.setTag("appointment-reminder");
        
        // 构建消息内容
        String messageBody = String.format("{\"appointmentId\":\"%s\",\"userId\":\"%s\",\"appointmentTime\":%d,\"appointmentType\":\"%s\"}", 
                appointmentId, userId, appointmentTime, appointmentType);
        message.setBody(messageBody);
        
        message.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        message.setDeliverTimestamp(remindTime);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("businessType", "appointmentReminder");
        properties.put("appointmentId", appointmentId);
        properties.put("userId", userId);
        message.setProperties(properties);
        
        // 发送延迟消息
        delayMessageSender.sendDelayMessage(message);
        
        logger.info("预约提醒的延迟消息已发送，预约ID: {}, 用户ID: {}, 消息ID: {}", 
                appointmentId, userId, message.getId());
    }

    /**
     * 场景四：分布式定时任务
     * 使用延迟消息实现分布式定时任务
     * 
     * @param taskId 任务ID
     * @param taskType 任务类型
     * @param executeTime 执行时间戳
     * @param taskData 任务数据
     */
    public void scheduleDistributedTask(String taskId, String taskType, 
                                       long executeTime, String taskData) {
        logger.info("创建分布式定时任务的延迟消息，任务ID: {}, 任务类型: {}", taskId, taskType);
        
        long currentTime = System.currentTimeMillis();
        long delayTime = executeTime - currentTime;
        
        // 如果执行时间已过，则立即执行
        if (delayTime <= 0) {
            delayTime = 0;
        }
        
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId(taskId); // 使用任务ID作为消息ID，便于后续查询和管理
        message.setTopic("task-topic");
        message.setTag(taskType);
        message.setBody(taskData);
        message.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        message.setDeliverTimestamp(currentTime + delayTime);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("businessType", "distributedTask");
        properties.put("taskId", taskId);
        properties.put("taskType", taskType);
        properties.put("scheduleTime", String.valueOf(currentTime));
        properties.put("executeTime", String.valueOf(executeTime));
        message.setProperties(properties);
        
        // 发送延迟消息
        delayMessageSender.sendDelayMessage(message);
        
        logger.info("分布式定时任务的延迟消息已发送，任务ID: {}, 任务类型: {}, 消息ID: {}, 延迟时间: {}ms", 
                taskId, taskType, message.getId(), delayTime);
    }
}