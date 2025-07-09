package com.example.mq.rocketmq;

import com.example.mq.factory.MQFactory;
import com.example.mq.producer.MQProducer;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class RocketMQMessageTest extends BaseRocketMQTest {

    private static final String TOPIC = "test_topic";
    private static final String TAG = "test_tag";

    @Autowired
    private MQFactory mqFactory;

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
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        TestEvent event = new TestEvent();
        event.setMessage("同步消息测试");
        producer.syncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);
    }

    @Test
    public void testAsyncSend() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> receivedMessages = new ArrayList<>();
        AtomicLong receiveTime = new AtomicLong(0);

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, message -> {
            receivedMessages.add(message);
            receiveTime.set(System.currentTimeMillis());
            latch.countDown();
        });

        // 等待消费者启动完成
        Thread.sleep(2000);

        // 发送异步消息
        TestEvent event = new TestEvent();
        event.setMessage("异步消息测试");
        long sendTime = System.currentTimeMillis();
        producer.asyncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);

        // 等待接收消息，最多等待20秒
        assertTrue(latch.await(20, TimeUnit.SECONDS), "消息接收超时");
        assertTrue(!receivedMessages.isEmpty(), "未收到消息");
    }

    @Test
    public void testDelayMessage() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> receivedMessages = new ArrayList<>();
        AtomicLong receiveTime = new AtomicLong(0);

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, message -> {
            receivedMessages.add(message);
            receiveTime.set(System.currentTimeMillis());
            latch.countDown();
        });

        // 等待消费者启动完成
        Thread.sleep(2000);

        // 发送延迟消息，延迟5秒
        TestEvent event = new TestEvent();
        event.setMessage("延迟消息测试");
        long sendTime = System.currentTimeMillis();
        producer.asyncSendDelay(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event, 5);

        // 等待接收消息
        assertTrue(latch.await(30, TimeUnit.SECONDS), "消息接收超时");
        assertTrue(!receivedMessages.isEmpty(), "未收到消息");

        // 验证消息内容
        String receivedMessage = receivedMessages.get(0);
        String expectedJson = String.format("{\"message\":\"%s\",\"tag\":\"%s\",\"topic\":\"%s\"}", 
            "延迟消息测试", TAG, TOPIC);
        assertTrue(receivedMessage.equals(expectedJson), 
            String.format("消息内容不匹配，期望：%s，实际：%s", expectedJson, receivedMessage));

        // 验证延迟时间（由于RocketMQ使用预设的延迟级别，这里验证延迟时间是否在合理范围内）
        long delayTime = receiveTime.get() - sendTime;
        log.info("消息延迟时间：{}毫秒", delayTime);
        // 由于5秒的延迟使用level 2（5秒延迟级别），所以延迟时间应该在4-7秒之间
        assertTrue(delayTime >= 4000 && delayTime <= 7000, 
            String.format("延迟时间不在预期范围内：%d毫秒", delayTime));
    }

    @Test
    public void testMessageOrder() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(3);
        List<String> receivedMessages = new ArrayList<>();

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, message -> {
            receivedMessages.add(message);
            latch.countDown();
        });

        // 等待消费者启动完成
        Thread.sleep(2000);

        // 发送三条顺序消息
        for (int i = 1; i <= 3; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("顺序消息测试" + i);
            producer.syncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);
        }

        // 等待接收消息
        assertTrue(latch.await(30, TimeUnit.SECONDS), "消息接收超时");
        assertTrue(receivedMessages.size() == 3, "未收到全部消息");

        // 验证消息顺序
        for (int i = 0; i < 3; i++) {
            assertTrue(receivedMessages.get(i).contains("顺序消息测试" + (i + 1)), "消息顺序不正确");
        }
    }
}