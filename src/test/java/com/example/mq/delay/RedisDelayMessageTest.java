package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.delay.adapter.RedisAdapter;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.delay.model.MessageStatusEnum;
import com.example.mq.enums.MQTypeEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Redis延迟消息测试
 * 专门测试Redis的延迟消息功能
 */
@ExtendWith(MockitoExtension.class)
public class RedisDelayMessageTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private StringRedisTemplate messageRedisTemplate;

    private RedisAdapter redisAdapter;
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

        // 初始化Redis适配器
        redisAdapter = new RedisAdapter(messageRedisTemplate);

        // 初始化适配器列表
        List<com.example.mq.delay.adapter.MQAdapter> adapters = new ArrayList<>();
        adapters.add(redisAdapter);

        // 初始化Redis模板（仅在需要时设置）
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);

        // 初始化延迟消息发送器
        delayMessageSender = new DelayMessageSender(redisTemplate, adapters, mqConfig);
    }

    @Test
    public void testSendRedisDelayMessage() throws Exception {
        // 测试Redis延迟消息发送
        String topic = "redis-delay-topic";
        String tag = "redis-delay-tag";
        String body = "Redis delay message test";
        String mqType = MQTypeEnum.REDIS.getType();
        long delayTime = 15000; // 15秒延迟

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(topic, tag, body, mqType, delayTime);

        // 验证消息ID不为空
        assertNotNull(messageId);
        
        // 验证Redis操作
        verify(valueOperations).set(anyString(), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void testRedisDelayMessageWithProperties() throws Exception {
        // 测试带属性的Redis延迟消息
        DelayMessage message = new DelayMessage();
        message.setId("redis-msg-001");
        message.setTopic("redis-topic");
        message.setTag("redis-tag");
        message.setBody("Redis message with properties");
        message.setMqTypeEnum(MQTypeEnum.REDIS);
        message.setDeliverTimestamp(System.currentTimeMillis() + 10000); // 10秒后投递

        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("priority", "high");
        properties.put("source", "redis-test");
        properties.put("channel", "test.redis.channel");
        message.setProperties(properties);

        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(message);

        // 验证消息ID
        assertEquals("redis-msg-001", messageId);
        
        // 验证Redis操作
        verify(valueOperations).set(contains(messageId), anyString());
        verify(zSetOperations).add(anyString(), eq(messageId), any(Double.class));
    }

    @Test
    public void testRedisMessageDelivery() throws Exception {
        // 测试Redis消息投递
        long pastTime = System.currentTimeMillis() - 1000; // 1秒前
        DelayMessage expiredMessage = new DelayMessage();
        expiredMessage.setId("redis-expired-msg-001");
        expiredMessage.setTopic("redis-expired-topic");
        expiredMessage.setTag("redis-expired-tag");
        expiredMessage.setBody("Redis expired message");
        expiredMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        expiredMessage.setDeliverTimestamp(pastTime);
        expiredMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        // 设置Redis通道
        Map<String, String> properties = new HashMap<>();
        properties.put("channel", "test.redis.channel");
        expiredMessage.setProperties(properties);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(expiredMessage);
        
        // 模拟Redis返回过期消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("redis-expired-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("redis-expired-msg-001"))).thenReturn(messageJson);
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被发送到Redis
        verify(messageRedisTemplate).convertAndSend(eq("test.redis.channel"), eq("Redis expired message"));
        
        // 验证消息状态被更新
        verify(valueOperations).set(contains("redis-expired-msg-001"), contains("DELIVERED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("redis-expired-msg-001"));
    }

    @Test
    public void testRedisMessageDeliveryWithoutChannel() throws Exception {
        // 测试没有指定通道的Redis消息投递
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage expiredMessage = new DelayMessage();
        expiredMessage.setId("redis-no-channel-msg-001");
        expiredMessage.setTopic("redis-no-channel-topic");
        expiredMessage.setTag("redis-no-channel-tag");
        expiredMessage.setBody("Redis message without channel");
        expiredMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        expiredMessage.setDeliverTimestamp(pastTime);
        expiredMessage.setStatusEnum(MessageStatusEnum.WAITING);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(expiredMessage);
        
        // 模拟Redis返回过期消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("redis-no-channel-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("redis-no-channel-msg-001"))).thenReturn(messageJson);
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被发送到Redis（使用topic作为通道）
        verify(messageRedisTemplate).convertAndSend(eq("redis-no-channel-topic"), eq("Redis message without channel"));
        
        // 验证消息状态被更新
        verify(valueOperations).set(contains("redis-no-channel-msg-001"), contains("DELIVERED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("redis-no-channel-msg-001"));
    }

    @Test
    public void testRedisMessageRetry() throws Exception {
        // 测试Redis消息重试机制
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage failedMessage = new DelayMessage();
        failedMessage.setId("redis-failed-msg-001");
        failedMessage.setTopic("redis-failed-topic");
        failedMessage.setTag("redis-failed-tag");
        failedMessage.setBody("Redis failed message");
        failedMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        failedMessage.setDeliverTimestamp(pastTime);
        failedMessage.setStatusEnum(MessageStatusEnum.WAITING);
        failedMessage.setRetryCount(0);
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(failedMessage);
        
        // 模拟Redis返回失败消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("redis-failed-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("redis-failed-msg-001"))).thenReturn(messageJson);
        
        // 模拟Redis发送失败
        doThrow(new RuntimeException("Redis send failed"))
                .when(messageRedisTemplate).convertAndSend(anyString(), anyString());
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被尝试发送
        verify(messageRedisTemplate).convertAndSend(anyString(), anyString());
        
        // 验证消息状态被更新（重试计数增加）
        verify(valueOperations, atLeastOnce()).set(contains("redis-failed-msg-001"), anyString());
        
        // 验证消息重新加入队列（用于重试）
        verify(zSetOperations).add(anyString(), eq("redis-failed-msg-001"), any(Double.class));
    }

    @Test
    public void testRedisMaxRetryExceeded() throws Exception {
        // 测试Redis超过最大重试次数
        long pastTime = System.currentTimeMillis() - 1000;
        DelayMessage maxRetriedMessage = new DelayMessage();
        maxRetriedMessage.setId("redis-max-retry-msg-001");
        maxRetriedMessage.setTopic("redis-max-retry-topic");
        maxRetriedMessage.setTag("redis-max-retry-tag");
        maxRetriedMessage.setBody("Redis max retry message");
        maxRetriedMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        maxRetriedMessage.setDeliverTimestamp(pastTime);
        maxRetriedMessage.setStatusEnum(MessageStatusEnum.WAITING);
        maxRetriedMessage.setRetryCount(3); // 已达到最大重试次数
        
        String messageJson = com.alibaba.fastjson.JSON.toJSONString(maxRetriedMessage);
        
        // 模拟Redis返回超过重试次数的消息
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("redis-max-retry-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class)))
                .thenReturn(expiredMessageIds);
        when(valueOperations.get(contains("redis-max-retry-msg-001"))).thenReturn(messageJson);
        
        // 模拟Redis发送失败
        doThrow(new RuntimeException("Redis send failed"))
                .when(messageRedisTemplate).convertAndSend(anyString(), anyString());
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被尝试发送
        verify(messageRedisTemplate).convertAndSend(anyString(), anyString());
        
        // 验证消息状态被标记为失败
        verify(valueOperations).set(contains("redis-max-retry-msg-001"), contains("FAILED"));
        
        // 验证消息从队列中移除
        verify(zSetOperations).remove(anyString(), eq("redis-max-retry-msg-001"));
    }

    @Test
    public void testRedisAdapterDirectSend() throws Exception {
        // 测试Redis适配器直接发送消息
        DelayMessage message = new DelayMessage();
        message.setId("redis-direct-msg-001");
        message.setTopic("redis-direct-topic");
        message.setTag("redis-direct-tag");
        message.setBody("Redis direct send message");
        message.setMqTypeEnum(MQTypeEnum.REDIS);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("channel", "direct.redis.channel");
        properties.put("priority", "1");
        message.setProperties(properties);
        
        // 直接调用适配器发送消息
        boolean result = redisAdapter.send(message);
        
        // 验证发送成功
        assertTrue(result);
        
        // 验证RedisTemplate被调用
        verify(messageRedisTemplate).convertAndSend(eq("direct.redis.channel"), eq("Redis direct send message"));
    }

    @Test
    public void testRedisAdapterGetMQType() {
        // 测试Redis适配器获取MQ类型
        String mqType = redisAdapter.getMQType();
        assertEquals(MQTypeEnum.REDIS.getType(), mqType);
    }

    @Test
    public void testRedisCacheMessage() throws Exception {
        // 测试Redis缓存消息功能
        DelayMessage cacheMessage = new DelayMessage();
        cacheMessage.setId("redis-cache-msg-001");
        cacheMessage.setTopic("redis-cache-topic");
        cacheMessage.setTag("redis-cache-tag");
        cacheMessage.setBody("Redis cache message test");
        cacheMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        
        // 设置缓存相关属性
        Map<String, String> properties = new HashMap<>();
        properties.put("cache-key", "test:cache:key");
        properties.put("cache-ttl", "3600");
        properties.put("channel", "cache.notification.channel");
        cacheMessage.setProperties(properties);
        
        // 发送缓存消息
        boolean result = redisAdapter.send(cacheMessage);
        
        // 验证发送成功
        assertTrue(result);
        
        // 验证Redis发布订阅被调用
        verify(messageRedisTemplate).convertAndSend(eq("cache.notification.channel"), eq("Redis cache message test"));
    }

    @Test
    public void testRedisListMessage() throws Exception {
        // 测试Redis列表消息功能
        DelayMessage listMessage = new DelayMessage();
        listMessage.setId("redis-list-msg-001");
        listMessage.setTopic("redis-list-topic");
        listMessage.setTag("redis-list-tag");
        listMessage.setBody("Redis list message test");
        listMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        
        // 设置列表相关属性
        Map<String, String> properties = new HashMap<>();
        properties.put("list-key", "test:message:list");
        properties.put("operation", "lpush");
        properties.put("channel", "list.notification.channel");
        listMessage.setProperties(properties);
        
        // 发送列表消息
        boolean result = redisAdapter.send(listMessage);
        
        // 验证发送成功
        assertTrue(result);
        
        // 验证Redis发布订阅被调用
        verify(messageRedisTemplate).convertAndSend(eq("list.notification.channel"), eq("Redis list message test"));
    }
}