package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.delay.adapter.ActiveMQAdapter;
import com.example.mq.delay.adapter.RocketMQAdapter;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.delay.model.MessageStatusEnum;
import com.example.mq.enums.MQTypeEnum;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.jms.core.JmsTemplate;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 延迟消息集成测试
 * 测试延迟消息的完整功能，包括发送、延迟投递、重试机制等
 */
@ExtendWith(MockitoExtension.class)
public class DelayMessageIntegrationTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private DefaultMQProducer rocketMQProducer;

    @Mock
    private JmsTemplate jmsTemplate;

    private RocketMQAdapter rocketMQAdapter;
    private ActiveMQAdapter activeMQAdapter;
    private DelayMessageSender delayMessageSender;
    private MQConfig mqConfig;

    @BeforeEach
    public void setup() throws Exception {
        // 初始化配置
        mqConfig = new MQConfig();
        MQConfig.DelayMessageProperties delayProps = new MQConfig.DelayMessageProperties();
        delayProps.setRedisKeyPrefix("test_delay_message:");
        delayProps.setScanInterval(100);
        delayProps.setBatchSize(10);
        delayProps.setMessageExpireTime(7 * 24 * 60 * 60 * 1000L); // 7天
        
        // 重试配置
        MQConfig.DelayMessageProperties.RetryConfig retryConfig = new MQConfig.DelayMessageProperties.RetryConfig();
        retryConfig.setMaxRetries(3);
        retryConfig.setRetryInterval(1000);
        retryConfig.setRetryMultiplier(2);
        delayProps.setRetry(retryConfig);
        
        mqConfig.setDelay(delayProps);

        // 初始化适配器
        rocketMQAdapter = new RocketMQAdapter("test-producer-group", "localhost:9876");
        // 使用反射将Mock的producer注入到adapter中
        java.lang.reflect.Field producerField = RocketMQAdapter.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(rocketMQAdapter, rocketMQProducer);
        
        activeMQAdapter = new ActiveMQAdapter(jmsTemplate);

        // 初始化适配器列表
        List<com.example.mq.delay.adapter.MQAdapter> adapters = new ArrayList<>();
        adapters.add(rocketMQAdapter);
        adapters.add(activeMQAdapter);

        // 初始化Redis模板
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);

        // 初始化延迟消息发送器
        delayMessageSender = new DelayMessageSender(redisTemplate, adapters, mqConfig);
    }

    @Test
    public void testSendDelayMessageWithShortDelay() throws Exception {
        // 测试短延迟消息发送
        String topic = "test-topic";
        String tag = "test-tag";
        String body = "Short delay message";
        String mqType = MQTypeEnum.ROCKET_MQ.getType();
        long delayTime = 1000; // 1秒延迟

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(topic, tag, body, mqType, delayTime);

        // 验证消息ID不为空
        assertNotNull(messageId);
        
        // 验证Redis操作
        verify(valueOperations).set(anyString(), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void testSendDelayMessageWithLongDelay() throws Exception {
        // 测试长延迟消息发送
        DelayMessage message = new DelayMessage();
        message.setId("long-delay-msg-001");
        message.setTopic("long-delay-topic");
        message.setTag("long-delay-tag");
        message.setBody("Long delay message");
        message.setMqTypeEnum(MQTypeEnum.ACTIVE_MQ);
        message.setDeliverTimestamp(System.currentTimeMillis() + 60000); // 1分钟后投递

        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("priority", "high");
        properties.put("source", "integration-test");
        message.setProperties(properties);

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(message);

        // 验证消息ID
        assertEquals("long-delay-msg-001", messageId);
        
        // 验证Redis操作
        verify(valueOperations).set(contains(messageId), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void testScanAndDeliverExpiredMessages() throws Exception {
        // 模拟过期消息
        long pastTime = System.currentTimeMillis() - 5000; // 5秒前
        DelayMessage expiredMessage = new DelayMessage();
        expiredMessage.setId("expired-msg-001");
        expiredMessage.setTopic("expired-topic");
        expiredMessage.setTag("expired-tag");
        expiredMessage.setBody("Expired message");
        expiredMessage.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        expiredMessage.setDeliverTimestamp(pastTime);
        expiredMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(expiredMessage);
        
        // 模拟Redis返回过期消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("expired-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("expired-msg-001"))).thenReturn(messageJson);
        
        // 模拟RocketMQ发送成功
        when(rocketMQProducer.send(any(org.apache.rocketmq.common.message.Message.class)))
                .thenReturn(mock(org.apache.rocketmq.client.producer.SendResult.class));
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被发送到RocketMQ
        verify(rocketMQProducer).send(any(org.apache.rocketmq.common.message.Message.class));
        
        // 验证消息状态被更新
        verify(valueOperations).set(contains("expired-msg-001"), contains("DELIVERED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("expired-msg-001"));
    }

    @Test
    public void testMessageRetryMechanism() throws Exception {
        // 模拟发送失败的消息
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage failedMessage = new DelayMessage();
        failedMessage.setId("failed-msg-001");
        failedMessage.setTopic("failed-topic");
        failedMessage.setTag("failed-tag");
        failedMessage.setBody("Failed message");
        failedMessage.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        failedMessage.setDeliverTimestamp(pastTime);
        failedMessage.setStatusEnum(MessageStatusEnum.WAITING);
        failedMessage.setRetryCount(0);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(failedMessage);
        
        // 模拟Redis返回失败消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("failed-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("failed-msg-001"))).thenReturn(messageJson);
        
        // 模拟RocketMQ发送失败
        when(rocketMQProducer.send(any(org.apache.rocketmq.common.message.Message.class)))
                .thenThrow(new RuntimeException("Send failed"));
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被尝试发送
        verify(rocketMQProducer).send(any(org.apache.rocketmq.common.message.Message.class));
        
        // 验证消息状态被更新（重试计数增加）
        verify(valueOperations, atLeastOnce()).set(contains("failed-msg-001"), anyString());
        
        // 验证消息重新加入队列（用于重试）
        verify(zSetOperations).add(anyString(), eq("failed-msg-001"), any(Double.class));
    }

    @Test
    public void testMaxRetryExceeded() throws Exception {
        // 模拟超过最大重试次数的消息
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage maxRetriedMessage = new DelayMessage();
        maxRetriedMessage.setId("max-retry-msg-001");
        maxRetriedMessage.setTopic("max-retry-topic");
        maxRetriedMessage.setTag("max-retry-tag");
        maxRetriedMessage.setBody("Max retry message");
        maxRetriedMessage.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        maxRetriedMessage.setDeliverTimestamp(pastTime);
        maxRetriedMessage.setStatusEnum(MessageStatusEnum.WAITING);
        maxRetriedMessage.setRetryCount(3); // 已达到最大重试次数
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(maxRetriedMessage);
        
        // 模拟Redis返回超过重试次数的消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("max-retry-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("max-retry-msg-001"))).thenReturn(messageJson);
        
        // 模拟RocketMQ发送失败
        when(rocketMQProducer.send(any(org.apache.rocketmq.common.message.Message.class)))
                .thenThrow(new RuntimeException("Send failed"));
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被尝试发送
        verify(rocketMQProducer).send(any(org.apache.rocketmq.common.message.Message.class));
        
        // 验证消息状态被标记为失败
        verify(valueOperations).set(contains("max-retry-msg-001"), contains("FAILED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("max-retry-msg-001"));
    }

    @Test
    public void testMultipleMQTypesDelivery() throws Exception {
        // 测试多种MQ类型的消息投递
        long pastTime = System.currentTimeMillis() - 1000;
        
        // RocketMQ消息
        DelayMessage rocketMQMessage = new DelayMessage();
        rocketMQMessage.setId("rocketmq-msg-001");
        rocketMQMessage.setTopic("rocketmq-topic");
        rocketMQMessage.setTag("rocketmq-tag");
        rocketMQMessage.setBody("RocketMQ message");
        rocketMQMessage.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        rocketMQMessage.setDeliverTimestamp(pastTime);
        rocketMQMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        // ActiveMQ消息
        DelayMessage activeMQMessage = new DelayMessage();
        activeMQMessage.setId("activemq-msg-001");
        activeMQMessage.setTopic("activemq-topic");
        activeMQMessage.setTag("activemq-tag");
        activeMQMessage.setBody("ActiveMQ message");
        activeMQMessage.setMqTypeEnum(MQTypeEnum.ACTIVE_MQ);
        activeMQMessage.setDeliverTimestamp(pastTime);
        activeMQMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        String rocketMQJson = com.alibaba.fastjson.JSON.toJSONString(rocketMQMessage);
        String activeMQJson = com.alibaba.fastjson.JSON.toJSONString(activeMQMessage);
        
        // 模拟Redis返回两种类型的消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("rocketmq-msg-001");
        expiredMessageIds.add("activemq-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("rocketmq-msg-001"))).thenReturn(rocketMQJson);
        when(valueOperations.get(contains("activemq-msg-001"))).thenReturn(activeMQJson);
        
        // 模拟发送成功
        when(rocketMQProducer.send(any(org.apache.rocketmq.common.message.Message.class)))
                .thenReturn(mock(org.apache.rocketmq.client.producer.SendResult.class));
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证RocketMQ消息被发送
        verify(rocketMQProducer).send(any(org.apache.rocketmq.common.message.Message.class));
        
        // 验证ActiveMQ消息被发送
        verify(jmsTemplate).send(anyString(), any());
        
        // 验证两条消息都被标记为已投递
        verify(valueOperations).set(contains("rocketmq-msg-001"), contains("DELIVERED"));
        verify(valueOperations).set(contains("activemq-msg-001"), contains("DELIVERED"));
    }

    @Test
    public void testMessageCleanup() throws Exception {
        // 测试过期消息清理功能
        long expiredTime = System.currentTimeMillis() - (8 * 24 * 60 * 60 * 1000L); // 8天前
        
        DelayMessage expiredDeliveredMessage = new DelayMessage();
        expiredDeliveredMessage.setId("expired-delivered-msg");
        expiredDeliveredMessage.setTopic("expired-topic");
        expiredDeliveredMessage.setTag("expired-tag");
        expiredDeliveredMessage.setBody("Expired delivered message");
        expiredDeliveredMessage.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        expiredDeliveredMessage.setDeliverTimestamp(expiredTime);
        expiredDeliveredMessage.setStatusEnum(MessageStatusEnum.DELIVERED);
        
        String expiredJson = com.alibaba.fastjson.JSON.toJSONString(expiredDeliveredMessage);
        
        // 模拟Redis返回过期消息键
        Set<String> messageKeys = new HashSet<>();
        messageKeys.add("test_delay_message:expired-delivered-msg");
        when(redisTemplate.keys(anyString())).thenReturn(messageKeys);
        when(valueOperations.get("test_delay_message:expired-delivered-msg")).thenReturn(expiredJson);
        
        // 执行清理
        delayMessageSender.cleanupExpiredMessages();
        
        // 验证过期消息被删除
        verify(redisTemplate).delete("test_delay_message:expired-delivered-msg");
        verify(zSetOperations).remove(anyString(), eq("expired-delivered-msg"));
    }
}