package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.delay.adapter.RabbitMQAdapter;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.delay.model.MessageStatusEnum;
import com.example.mq.enums.MQTypeEnum;
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
 * RabbitMQ延迟消息测试
 * 专门测试RabbitMQ的延迟消息功能
 */
@ExtendWith(MockitoExtension.class)
public class RabbitMQDelayMessageTest {

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

        // 初始化RabbitMQ适配器
        rabbitMQAdapter = new RabbitMQAdapter(rabbitTemplate);

        // 初始化适配器列表
        List<com.example.mq.delay.adapter.MQAdapter> adapters = new ArrayList<>();
        adapters.add(rabbitMQAdapter);

        // 初始化Redis模板（仅在需要时设置）
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);

        // 初始化延迟消息发送器
        delayMessageSender = new DelayMessageSender(redisTemplate, adapters, mqConfig);
    }

    @Test
    public void testSendRabbitMQDelayMessage() throws Exception {
        // 测试RabbitMQ延迟消息发送
        String topic = "rabbitmq-delay-topic";
        String tag = "rabbitmq-delay-tag";
        String body = "RabbitMQ delay message test";
        String mqType = MQTypeEnum.RABBIT_MQ.getType();
        long delayTime = 5000; // 5秒延迟

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(topic, tag, body, mqType, delayTime);

        // 验证消息ID不为空
        assertNotNull(messageId);
        
        // 验证Redis操作
        verify(valueOperations).set(anyString(), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void testRabbitMQDelayMessageWithProperties() throws Exception {
        // 测试带属性的RabbitMQ延迟消息
        DelayMessage message = new DelayMessage();
        message.setId("rabbitmq-msg-001");
        message.setTopic("rabbitmq-topic");
        message.setTag("rabbitmq-tag");
        message.setBody("RabbitMQ message with properties");
        message.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        message.setDeliverTimestamp(System.currentTimeMillis() + 10000); // 10秒后投递

        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("priority", "high");
        properties.put("source", "rabbitmq-test");
        properties.put("routing-key", "test.routing.key");
        message.setProperties(properties);

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(message);

        // 验证消息ID
        assertEquals("rabbitmq-msg-001", messageId);
        
        // 验证Redis操作
        verify(valueOperations).set(contains(messageId), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void testRabbitMQMessageDelivery() throws Exception {
        // 测试RabbitMQ消息投递
        long pastTime = System.currentTimeMillis() - 1000; // 1秒前
        DelayMessage expiredMessage = new DelayMessage();
        expiredMessage.setId("rabbitmq-expired-msg-001");
        expiredMessage.setTopic("rabbitmq-expired-topic");
        expiredMessage.setTag("rabbitmq-expired-tag");
        expiredMessage.setBody("RabbitMQ expired message");
        expiredMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        expiredMessage.setDeliverTimestamp(pastTime);
        expiredMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("custom-header", "test-value");
        expiredMessage.setProperties(properties);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(expiredMessage);
        
        // 模拟Redis返回过期消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("rabbitmq-expired-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("rabbitmq-expired-msg-001"))).thenReturn(messageJson);
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被发送到RabbitMQ（使用tag作为路由键）
        verify(rabbitTemplate).send(eq("rabbitmq-expired-topic"), eq("rabbitmq-expired-tag"), any());
        
        // 验证消息状态被更新
        verify(valueOperations).set(contains("rabbitmq-expired-msg-001"), contains("DELIVERED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("rabbitmq-expired-msg-001"));
    }

    @Test
    public void testRabbitMQMessageDeliveryWithoutRoutingKey() throws Exception {
        // 测试没有路由键的RabbitMQ消息投递
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage expiredMessage = new DelayMessage();
        expiredMessage.setId("rabbitmq-no-routing-msg-001");
        expiredMessage.setTopic("rabbitmq-no-routing-topic");
        expiredMessage.setTag("rabbitmq-no-routing-tag");
        expiredMessage.setBody("RabbitMQ message without routing key");
        expiredMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        expiredMessage.setDeliverTimestamp(pastTime);
        expiredMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(expiredMessage);
        
        // 模拟Redis返回过期消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("rabbitmq-no-routing-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("rabbitmq-no-routing-msg-001"))).thenReturn(messageJson);
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被发送到RabbitMQ（使用tag作为路由键）
        verify(rabbitTemplate).send(eq("rabbitmq-no-routing-topic"), eq("rabbitmq-no-routing-tag"), any());
        
        // 验证消息状态被更新
        verify(valueOperations).set(contains("rabbitmq-no-routing-msg-001"), contains("DELIVERED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("rabbitmq-no-routing-msg-001"));
    }

    @Test
    public void testRabbitMQMessageRetry() throws Exception {
        // 测试RabbitMQ消息重试机制
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage failedMessage = new DelayMessage();
        failedMessage.setId("rabbitmq-failed-msg-001");
        failedMessage.setTopic("rabbitmq-failed-topic");
        failedMessage.setTag("rabbitmq-failed-tag");
        failedMessage.setBody("RabbitMQ failed message");
        failedMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        failedMessage.setDeliverTimestamp(pastTime);
        failedMessage.setStatusEnum(MessageStatusEnum.WAITING);
        failedMessage.setRetryCount(0);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(failedMessage);
        
        // 模拟Redis返回失败消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("rabbitmq-failed-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("rabbitmq-failed-msg-001"))).thenReturn(messageJson);
        
        // 模拟RabbitMQ发送失败
        doThrow(new RuntimeException("RabbitMQ send failed"))
                .when(rabbitTemplate).send(anyString(), anyString(), any());
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被尝试发送
        verify(rabbitTemplate).send(anyString(), anyString(), any());
        
        // 验证消息状态被更新（重试计数增加）
        verify(valueOperations, atLeastOnce()).set(contains("rabbitmq-failed-msg-001"), anyString());
        
        // 验证消息重新加入队列（用于重试）
        verify(zSetOperations).add(anyString(), eq("rabbitmq-failed-msg-001"), any(Double.class));
    }

    @Test
    public void testRabbitMQMaxRetryExceeded() throws Exception {
        // 测试RabbitMQ超过最大重试次数
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage maxRetriedMessage = new DelayMessage();
        maxRetriedMessage.setId("rabbitmq-max-retry-msg-001");
        maxRetriedMessage.setTopic("rabbitmq-max-retry-topic");
        maxRetriedMessage.setTag("rabbitmq-max-retry-tag");
        maxRetriedMessage.setBody("RabbitMQ max retry message");
        maxRetriedMessage.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        maxRetriedMessage.setDeliverTimestamp(pastTime);
        maxRetriedMessage.setStatusEnum(MessageStatusEnum.WAITING);
        maxRetriedMessage.setRetryCount(3); // 已达到最大重试次数
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(maxRetriedMessage);
        
        // 模拟Redis返回超过重试次数的消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("rabbitmq-max-retry-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("rabbitmq-max-retry-msg-001"))).thenReturn(messageJson);
        
        // 模拟RabbitMQ发送失败
        doThrow(new RuntimeException("RabbitMQ send failed"))
                .when(rabbitTemplate).send(anyString(), anyString(), any());
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被尝试发送
        verify(rabbitTemplate).send(anyString(), anyString(), any());
        
        // 验证消息状态被标记为失败
        verify(valueOperations).set(contains("rabbitmq-max-retry-msg-001"), contains("FAILED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("rabbitmq-max-retry-msg-001"));
    }

    @Test
    public void testRabbitMQAdapterDirectSend() throws Exception {
        // 测试RabbitMQ适配器直接发送消息
        DelayMessage message = new DelayMessage();
        message.setId("rabbitmq-direct-msg-001");
        message.setTopic("rabbitmq-direct-topic");
        message.setTag("rabbitmq-direct-tag");
        message.setBody("RabbitMQ direct send message");
        message.setMqTypeEnum(MQTypeEnum.RABBIT_MQ);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("routing-key", "direct.routing.key");
        properties.put("priority", "1");
        message.setProperties(properties);
        
        // 直接调用适配器发送消息
        boolean result = rabbitMQAdapter.send(message);
        
        // 验证发送成功
        assertTrue(result);
        
        // 验证RabbitTemplate被调用（使用tag作为路由键）
        verify(rabbitTemplate).send(eq("rabbitmq-direct-topic"), eq("rabbitmq-direct-tag"), any());
    }

    @Test
    public void testRabbitMQAdapterGetMQType() {
        // 测试RabbitMQ适配器获取MQ类型
        String mqType = rabbitMQAdapter.getMQType();
        assertEquals(MQTypeEnum.RABBIT_MQ.getType(), mqType);
    }
}