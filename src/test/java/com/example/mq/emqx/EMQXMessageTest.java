package com.example.mq.emqx;

import com.example.mq.enums.MQTypeEnum;
import com.example.mq.factory.MQFactory;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@org.junit.jupiter.api.Disabled("EMQX not installed")
public class EMQXMessageTest extends BaseEMQXTest {

    @Autowired
    private MQFactory mqFactory;

    private static final String TOPIC = "test_topic";
    private static final String TAG = "test_tag";

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
    @EnabledIfSystemProperty(named = "mq.emqx.enabled", matches = "true")
    public void testSyncSend() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.EMQX);
        TestEvent event = new TestEvent();
        event.setMessage("同步消息测试");

        // 测试同步发送
        String messageId = producer.syncSend(MQTypeEnum.EMQX, TOPIC, TAG, event);
        assertNotNull(messageId, "消息ID不应为空");
    }

    @Test
    @EnabledIfSystemProperty(named = "mq.emqx.enabled", matches = "true")
    public void testAsyncSend() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.EMQX);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> receivedMessages = new ArrayList<>();

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.EMQX).subscribe(MQTypeEnum.EMQX, TOPIC, TAG, message -> {
            receivedMessages.add(message);
            latch.countDown();
        });

        // 异步发送消息
        TestEvent event = new TestEvent();
        event.setMessage("异步消息测试");
        producer.asyncSend(MQTypeEnum.EMQX, TOPIC, TAG, event);

        // 等待接收消息
        assertTrue(latch.await(5, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(1, receivedMessages.size(), "应该收到一条消息");
        assertTrue(receivedMessages.get(0).contains("异步消息测试"), "消息内容不匹配");
    }

    @Test
    @EnabledIfSystemProperty(named = "mq.emqx.enabled", matches = "true")
    public void testDelayMessage() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.EMQX);
        CountDownLatch latch = new CountDownLatch(1);
        List<Long> receiveTimestamps = new ArrayList<>();

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.EMQX).subscribe(MQTypeEnum.EMQX, TOPIC, TAG, message -> {
            receiveTimestamps.add(System.currentTimeMillis());
            latch.countDown();
        });

        // 发送延迟消息
        TestEvent event = new TestEvent();
        event.setMessage("延迟消息测试");
        long sendTime = System.currentTimeMillis();
        int delaySeconds = 3;
        producer.asyncSendDelay(MQTypeEnum.EMQX, TOPIC, TAG, event, delaySeconds);

        // 等待接收消息
        assertTrue(latch.await(delaySeconds + 2, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(1, receiveTimestamps.size(), "应该收到一条消息");
        
        // 验证延迟时间
        long actualDelay = receiveTimestamps.get(0) - sendTime;
        assertTrue(actualDelay >= delaySeconds * 1000, 
                String.format("消息应该延迟至少%d秒，实际延迟%d毫秒", delaySeconds, actualDelay));
    }

    @Test
    @EnabledIfSystemProperty(named = "mq.emqx.enabled", matches = "true")
    public void testMessageOrder() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.EMQX);
        CountDownLatch latch = new CountDownLatch(3);
        List<String> receivedMessages = new ArrayList<>();

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.EMQX).subscribe(MQTypeEnum.EMQX, TOPIC, TAG, message -> {
            receivedMessages.add(message);
            latch.countDown();
        });

        // 发送三条消息
        for (int i = 1; i <= 3; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("消息" + i);
            producer.syncSend(MQTypeEnum.EMQX, TOPIC, TAG, event);
        }

        // 等待接收所有消息
        assertTrue(latch.await(5, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(3, receivedMessages.size(), "应该收到三条消息");

        // 验证消息顺序
        for (int i = 0; i < 3; i++) {
            assertTrue(receivedMessages.get(i).contains("消息" + (i + 1)), 
                    "消息顺序不正确，期望：消息" + (i + 1) + "，实际：" + receivedMessages.get(i));
        }
    }
}