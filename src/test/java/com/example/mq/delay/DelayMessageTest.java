package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.delay.adapter.ActiveMQAdapter;
import com.example.mq.delay.adapter.RocketMQAdapter;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.enums.MQTypeEnum;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jms.core.JmsTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DelayMessageTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private org.springframework.data.redis.core.ZSetOperations<String, String> zSetOperations;

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
        // 初始化适配器 - 使用反射设置Mock的producer
        rocketMQAdapter = new RocketMQAdapter("test-producer-group", "localhost:9876");
        // 使用反射将Mock的producer注入到adapter中
        java.lang.reflect.Field producerField = RocketMQAdapter.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(rocketMQAdapter, rocketMQProducer);
        
        activeMQAdapter = new ActiveMQAdapter(jmsTemplate);

        // 初始化配置
        mqConfig = new MQConfig();
        MQConfig.DelayMessageProperties delayProps = new MQConfig.DelayMessageProperties();
        delayProps.setRedisKeyPrefix("test_delay_message:");
        delayProps.setScanInterval(100);
        delayProps.setBatchSize(10);
        mqConfig.setDelay(delayProps);

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
    public void testSendDelayMessage() throws Exception {
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId("test-msg-001");
        message.setTopic("test-topic");
        message.setTag("test-tag");
        message.setBody("This is a test message");
        message.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        message.setDeliverTimestamp(System.currentTimeMillis() + 5000); // 5秒后投递

        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");
        message.setProperties(properties);

        // 发送延迟消息
        delayMessageSender.sendDelayMessage(message);

        // 验证Redis操作
        verify(valueOperations).set(anyString(), anyString());
        verify(zSetOperations).add(anyString(), anyString(), any(Double.class));
    }

    @Test
    public void testSendMessageWithDelay() throws Exception {
        // 测试使用延迟时间发送消息
        String topic = "test-topic";
        String tag = "test-tag";
        String body = "This is a test message with delay";
        MQTypeEnum mqType = MQTypeEnum.ACTIVE_MQ;
        long delayTime = 10000; // 10秒延迟

        // 发送延迟消息
        delayMessageSender.sendDelayMessage(topic, tag, body, mqType.getType(), delayTime);

        // 验证Redis操作
        verify(valueOperations).set(anyString(), anyString());
        verify(zSetOperations).add(anyString(), anyString(), any(Double.class));
    }

    @Test
    public void testScanAndDeliverMessages() throws Exception {
        // 模拟Redis中有过期消息
        long pastTime = System.currentTimeMillis() - 1000; // 1秒前的时间戳
        String messageJson = "{\"id\":\"expired-msg-001\",\"topic\":\"test-topic\",\"tag\":\"test-tag\",\"body\":\"Expired message\",\"mqTypeEnum\":\"ROCKET_MQ\",\"deliverTimestamp\":"+pastTime+",\"properties\":{\"key1\":\"value1\"}}";
        
        // 模拟Redis操作
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(valueOperations.get(anyString())).thenReturn(messageJson);
        
        // 模拟过期消息ID集合
        Set<String> expiredMessageIds = new HashSet<>();
        expiredMessageIds.add("expired-msg-001");
        when(zSetOperations.rangeByScore(anyString(), any(Double.class), any(Double.class))).thenReturn(expiredMessageIds);
        
        // 执行扫描和投递
        delayMessageSender.scanAndDeliverMessages();
        
        // 验证消息被发送到RocketMQ
        verify(rocketMQProducer).send(any(org.apache.rocketmq.common.message.Message.class));
    }
}