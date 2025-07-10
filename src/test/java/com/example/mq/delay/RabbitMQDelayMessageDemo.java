package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.delay.adapter.RabbitMQAdapter;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.delay.model.MessageStatusEnum;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * RabbitMQ延迟消息功能演示
 * 展示如何使用RabbitMQ发送和处理延迟消息
 */
@Slf4j
@ExtendWith(MockitoExtension.class)
public class RabbitMQDelayMessageDemo {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private RabbitTemplate rabbitTemplate;

    private RabbitMQAdapter rabbitMQAdapter;
    private DelayMessageSender delayMessageSender;
    private MQConfig mqConfig;

    @BeforeEach
    public void setup() throws Exception {
        // 初始化配置
        mqConfig = new MQConfig();
        MQConfig.DelayMessageProperties delayProps = new MQConfig.DelayMessageProperties();
        delayProps.setRedisKeyPrefix("demo_delay_message:");
        delayProps.setScanInterval(1000); // 1秒扫描间隔
        delayProps.setBatchSize(50);
        delayProps.setMessageExpireTime(24 * 60 * 60 * 1000L); // 24小时过期
        
        // 重试配置
        MQConfig.DelayMessageProperties.RetryConfig retryConfig = new MQConfig.DelayMessageProperties.RetryConfig();
        retryConfig.setMaxRetries(5);
        retryConfig.setRetryInterval(2000);
        retryConfig.setRetryMultiplier(2);
        delayProps.setRetry(retryConfig);
        
        mqConfig.setDelay(delayProps);

        // 初始化RabbitMQ适配器
        rabbitMQAdapter = new RabbitMQAdapter(rabbitTemplate);

        // 初始化适配器列表
        List<com.example.mq.delay.adapter.MQAdapter> adapters = new ArrayList<>();
        adapters.add(rabbitMQAdapter);

        // 初始化Redis模板
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);

        // 初始化延迟消息发送器
        delayMessageSender = new DelayMessageSender(redisTemplate, adapters, mqConfig);
    }

    @Test
    public void demoSendOrderTimeoutMessage() throws Exception {
        log.info("=== RabbitMQ延迟消息演示：订单超时处理 ===");
        
        // 场景：用户下单后30分钟未支付，发送超时提醒
        String orderId = "ORDER_" + System.currentTimeMillis();
        String topic = "order.timeout.exchange";
        String tag = "order.timeout.check";
        String message = String.format("订单超时检查：订单ID=%s，请及时处理", orderId);
        String mqType = MQTypeEnum.RABBIT_MQ.getType();
        long delayTime = 30 * 60 * 1000; // 30分钟延迟

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(topic, tag, message, mqType, delayTime);
        
        log.info("订单超时检查消息已发送：messageId={}, orderId={}, 延迟时间={}分钟", 
                messageId, orderId, delayTime / 60000);
        
        // 验证消息发送成功
        assertNotNull(messageId);
        verify(valueOperations).set(anyString(), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void demoSendPromotionMessage() throws Exception {
        log.info("=== RabbitMQ延迟消息演示：促销活动推送 ===");
        
        // 场景：用户注册后24小时发送促销消息
        DelayMessage promotionMessage = new DelayMessage();
        promotionMessage.setId("PROMO_" + UUID.randomUUID().toString());
        promotionMessage.setTopic("promotion.message.exchange");
        promotionMessage.setTag("new.user.promotion");
        promotionMessage.setBody("欢迎新用户！限时优惠券已发放，快来查看吧！");
        promotionMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        promotionMessage.setDeliverTimestamp(System.currentTimeMillis() + 24 * 60 * 60 * 1000); // 24小时后

        // 设置用户信息
        Map<String, String> properties = new HashMap<>();
        properties.put("userId", "USER_12345");
        properties.put("userType", "NEW_USER");
        properties.put("promotionType", "WELCOME_COUPON");
        promotionMessage.setProperties(properties);

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(promotionMessage);
        
        log.info("促销消息已发送：messageId={}, 用户ID={}, 投递时间={}", 
                messageId, properties.get("userId"), new Date(promotionMessage.getDeliverTimestamp()));
        
        // 验证消息发送成功
        assertEquals(promotionMessage.getId(), messageId);
        verify(valueOperations).set(contains(messageId), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void demoProcessExpiredMessages() throws Exception {
        log.info("=== RabbitMQ延迟消息演示：处理到期消息 ===");
        
        // 模拟一个已到期的消息
        long pastTime = System.currentTimeMillis() - 5000; // 5秒前
        DelayMessage expiredMessage = new DelayMessage();
        expiredMessage.setId("EXPIRED_MSG_" + System.currentTimeMillis());
        expiredMessage.setTopic("notification.exchange");
        expiredMessage.setTag("system.notification");
        expiredMessage.setBody("系统通知：您有一条重要消息待查看");
        expiredMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        expiredMessage.setDeliverTimestamp(pastTime);
        expiredMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        // 设置通知属性
        Map<String, String> properties = new HashMap<>();
        properties.put("notificationType", "SYSTEM_ALERT");
        properties.put("priority", "HIGH");
        expiredMessage.setProperties(properties);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(expiredMessage);
        
        // 模拟Redis返回过期消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add(expiredMessage.getId());
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains(expiredMessage.getId()))).thenReturn(messageJson);
        
        log.info("开始扫描和处理到期消息...");
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        log.info("到期消息处理完成：messageId={}, topic={}, tag={}", 
                expiredMessage.getId(), expiredMessage.getTopic(), expiredMessage.getTag());
        
        // 验证消息被发送到RabbitMQ
        verify(rabbitTemplate).send(eq("notification.exchange"), eq("system.notification"), any());
        
        // 验证消息状态被更新为已投递
        verify(valueOperations).set(contains(expiredMessage.getId()), contains("DELIVERED"));
        
        // 验证消息从延迟队列中移除
        verify(zSetOperations).remove(anyString(), eq(expiredMessage.getId()));
    }

    @Test
    public void demoMessageRetryMechanism() throws Exception {
        log.info("=== RabbitMQ延迟消息演示：消息重试机制 ===");
        
        // 模拟一个发送失败需要重试的消息
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage retryMessage = new DelayMessage();
        retryMessage.setId("RETRY_MSG_" + System.currentTimeMillis());
        retryMessage.setTopic("payment.notification.exchange");
        retryMessage.setTag("payment.failed");
        retryMessage.setBody("支付失败通知：订单支付异常，请重新尝试");
        retryMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        retryMessage.setDeliverTimestamp(pastTime);
        retryMessage.setStatusEnum(MessageStatusEnum.WAITING);
        retryMessage.setRetryCount(1); // 已重试1次
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(retryMessage);
        
        // 模拟Redis返回需要重试的消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add(retryMessage.getId());
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains(retryMessage.getId()))).thenReturn(messageJson);
        
        // 模拟RabbitMQ发送失败
        doThrow(new RuntimeException("RabbitMQ连接异常"))
                .when(rabbitTemplate).send(anyString(), anyString(), any());
        
        log.info("模拟消息发送失败，触发重试机制...");
        
        // 执行扫描和投递（会触发重试）
        delayMessageSender.scanAndDeliverMessages();
        
        log.info("消息重试处理完成：messageId={}, 当前重试次数={}", 
                retryMessage.getId(), retryMessage.getRetryCount());
        
        // 验证消息被尝试发送
        verify(rabbitTemplate).send(anyString(), anyString(), any());
        
        // 验证消息状态被更新（重试计数增加）
        verify(valueOperations, atLeastOnce()).set(contains(retryMessage.getId()), anyString());
        
        // 验证消息重新加入队列等待下次重试
        verify(zSetOperations).add(anyString(), eq(retryMessage.getId()), any(Double.class));
    }

    @Test
    public void demoRabbitMQAdapterFeatures() throws Exception {
        log.info("=== RabbitMQ延迟消息演示：适配器功能特性 ===");
        
        // 创建一个包含完整信息的消息
        DelayMessage fullMessage = new DelayMessage();
        fullMessage.setId("FULL_MSG_" + System.currentTimeMillis());
        fullMessage.setTopic("user.activity.exchange");
        fullMessage.setTag("user.login.notification");
        fullMessage.setBody("用户登录通知：检测到异常登录行为，请确认是否为本人操作");
        fullMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        
        // 设置丰富的消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("userId", "USER_98765");
        properties.put("loginIp", "192.168.1.100");
        properties.put("loginTime", String.valueOf(System.currentTimeMillis()));
        properties.put("deviceType", "MOBILE");
        properties.put("riskLevel", "MEDIUM");
        fullMessage.setProperties(properties);
        
        log.info("测试RabbitMQ适配器功能：");
        log.info("- 消息ID: {}", fullMessage.getId());
        log.info("- 交换机: {}", fullMessage.getTopic());
        log.info("- 路由键: {}", fullMessage.getTag());
        log.info("- 消息内容: {}", fullMessage.getBody());
        log.info("- 消息属性: {}", properties);
        
        // 测试适配器类型
        String mqType = rabbitMQAdapter.getMQType();
        log.info("- MQ类型: {}", mqType);
        assertEquals(MQTypeEnum.RABBIT_MQ.getType(), mqType);
        
        // 测试消息发送
        boolean sendResult = rabbitMQAdapter.send(fullMessage);
        log.info("- 发送结果: {}", sendResult ? "成功" : "失败");
        assertTrue(sendResult);
        
        // 验证RabbitTemplate被正确调用
        verify(rabbitTemplate).send(eq("user.activity.exchange"), eq("user.login.notification"), any());
        
        log.info("RabbitMQ适配器功能测试完成！");
    }
}