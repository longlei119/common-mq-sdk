package com.example.mq.redis;

import com.alibaba.fastjson.JSON;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.factory.MQFactory;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import com.example.mq.consumer.impl.RedisConsumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class RedisMessageTest extends BaseRedisTest {

    @Autowired
    private MQFactory mqFactory;

    private MQProducer producer;
    private RedisConsumer consumer;

    private static final String TOPIC = "test_topic";
    private static final String TAG = "test_tag";

    @BeforeEach
    public void setUp() {
        producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        consumer = (RedisConsumer) mqFactory.getConsumer(MQTypeEnum.REDIS);
    }

    @Data
    static class TestEvent extends MQEvent {
        private String message;

        @Override
        public String getTopic() {
            return TOPIC;
        }

        @Override
        public String getTag() {
            return TAG;
        }
    }

    @Test
    public void testSyncSend() {
        TestEvent event = new TestEvent();
        event.setMessage("同步消息测试");

        // 测试同步发送
        String messageId = producer.syncSend(MQTypeEnum.REDIS, TOPIC, TAG, event);
        assertNotNull(messageId, "消息ID不应为空");
    }

    @Test
    public void testAsyncSend() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        // 先订阅，等待一段时间确保监听器初始化完成
        consumer.subscribe(MQTypeEnum.REDIS, TOPIC, TAG, msg -> {
            log.info("收到异步消息: {}", msg);
            latch.countDown();
        });
        
        // 等待监听器初始化
        Thread.sleep(2000);

        // 发送消息
        TestEvent event = new TestEvent();
        event.setMessage("异步消息测试");
        producer.asyncSend(MQTypeEnum.REDIS, TOPIC, TAG, event);

        // 等待消息接收，超时时间设置为15秒
        assertTrue(latch.await(15, TimeUnit.SECONDS), "消息接收超时");
    }

    @Test
    public void testDelayMessage() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Long> receiveTimestamps = new ArrayList<>();

        // 订阅消息
        consumer.subscribe(MQTypeEnum.REDIS, TOPIC, TAG, message -> {
            receiveTimestamps.add(System.currentTimeMillis());
            latch.countDown();
        });

        // 发送延迟消息
        TestEvent event = new TestEvent();
        event.setMessage("延迟消息测试");
        long sendTime = System.currentTimeMillis();
        int delaySeconds = 3;
        producer.asyncSendDelay(MQTypeEnum.REDIS, TOPIC, TAG, event, delaySeconds);

        // 等待接收消息
        assertTrue(latch.await(delaySeconds + 2, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(1, receiveTimestamps.size(), "应该收到一条消息");
        
        // 验证延迟时间
        long actualDelay = receiveTimestamps.get(0) - sendTime;
        assertTrue(actualDelay >= delaySeconds * 1000, 
                String.format("消息应该延迟至少%d秒，实际延迟%d毫秒", delaySeconds, actualDelay));
    }

    @Test
    public void testMessageOrder() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        List<String> receivedMessages = new ArrayList<>();

        // 先订阅，等待一段时间确保监听器初始化完成
        consumer.subscribe(MQTypeEnum.REDIS, TOPIC, TAG, message -> {
            log.info("收到顺序消息: {}", message);
            receivedMessages.add(message);
            latch.countDown();
        });

        // 等待监听器初始化
        Thread.sleep(2000);

        // 发送三条消息
        for (int i = 1; i <= 3; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("消息" + i);
            log.info("发送顺序消息: {}", event.getMessage());
            producer.asyncSend(MQTypeEnum.REDIS, TOPIC, TAG, JSON.toJSONString(event));
            // 短暂等待确保消息按顺序发送
            Thread.sleep(100);
        }

        // 等待接收所有消息，增加超时时间
        assertTrue(latch.await(15, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(3, receivedMessages.size(), "应该收到三条消息");

        // 验证消息顺序
        for (int i = 0; i < 3; i++) {
            TestEvent receivedEvent = JSON.parseObject(receivedMessages.get(i), TestEvent.class);
            assertEquals("消息" + (i + 1), receivedEvent.getMessage(), 
                    "消息顺序不正确，期望：消息" + (i + 1) + "，实际：" + receivedEvent.getMessage());
        }
    }
}