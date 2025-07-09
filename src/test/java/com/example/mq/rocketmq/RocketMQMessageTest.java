package com.example.mq.rocketmq;

import com.example.mq.enums.MQTypeEnum;
import com.example.mq.factory.MQFactory;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class RocketMQMessageTest extends BaseRocketMQTest {

    private static final String TOPIC_PREFIX = "test_topic_";
    private static final String TAG_PREFIX = "test_tag_";

    @Autowired
    private MQFactory mqFactory;

    @Data
    static class TestEvent extends MQEvent {
        private String message;
        private String topic;
        private String tag;

        @Override
        public String getTopic() {
            return topic;
        }

        @Override
        public String getTag() {
            return tag;
        }
    }

    @Test
    public void testSyncSend() throws InterruptedException {
        String topic = TOPIC_PREFIX + "sync";
        String tag = TAG_PREFIX + "sync";
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> receivedMessages = new ArrayList<>();

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, topic, tag, message -> {
            receivedMessages.add(message);
            latch.countDown();
        });

        // 等待消费者启动完成
        Thread.sleep(2000);

        // 发送同步消息
        TestEvent event = new TestEvent();
        event.setMessage("同步消息测试");
        event.setTopic(topic);
        event.setTag(tag);
        event.setSendSuccessTime(new AtomicLong(0));
        producer.syncSend(MQTypeEnum.ROCKET_MQ, topic, tag, event);

        // 等待接收消息
        assertTrue(latch.await(20, TimeUnit.SECONDS), "消息接收超时");
        assertTrue(!receivedMessages.isEmpty(), "未收到消息");

        // 验证消息内容
        String receivedMessage = receivedMessages.get(0);
        com.alibaba.fastjson.JSONObject receivedJson = com.alibaba.fastjson.JSON.parseObject(receivedMessage);
        receivedJson.remove("sendSuccessTime");
        com.alibaba.fastjson.JSONObject expectedJson = com.alibaba.fastjson.JSON.parseObject(
            String.format("{\"message\":\"%s\",\"tag\":\"%s\",\"topic\":\"%s\"}",
                "同步消息测试", tag, topic));
        assertTrue(receivedJson.equals(expectedJson),
            String.format("消息内容不匹配，期望：%s，实际：%s", expectedJson.toString(), receivedJson.toString()));
    }

    @Test
    public void testAsyncSend() throws InterruptedException {
        String topic = TOPIC_PREFIX + "async";
        String tag = TAG_PREFIX + "async";
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> receivedMessages = new ArrayList<>();
        AtomicLong receiveTime = new AtomicLong(0);

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, topic, tag, message -> {
            receivedMessages.add(message);
            receiveTime.set(System.currentTimeMillis());
            latch.countDown();
        });

        // 等待消费者启动完成
        Thread.sleep(2000);

        // 发送异步消息
        TestEvent event = new TestEvent();
        event.setMessage("异步消息测试");
        event.setTopic(topic);
        event.setTag(tag);
        event.setSendSuccessTime(new AtomicLong(0));
        producer.asyncSend(MQTypeEnum.ROCKET_MQ, topic, tag, event);

        // 等待接收消息，最多等待20秒
        assertTrue(latch.await(20, TimeUnit.SECONDS), "消息接收超时");
        assertTrue(!receivedMessages.isEmpty(), "未收到消息");

        // 验证消息内容
        String receivedMessage = receivedMessages.get(0);
        com.alibaba.fastjson.JSONObject receivedJson = com.alibaba.fastjson.JSON.parseObject(receivedMessage);
        receivedJson.remove("sendSuccessTime");
        com.alibaba.fastjson.JSONObject expectedJson = com.alibaba.fastjson.JSON.parseObject(
            String.format("{\"message\":\"%s\",\"tag\":\"%s\",\"topic\":\"%s\"}",
                "异步消息测试", tag, topic));
        assertTrue(receivedJson.equals(expectedJson),
            String.format("消息内容不匹配，期望：%s，实际：%s", expectedJson.toString(), receivedJson.toString()));
    }

    @Test
    public void testDelayMessage() throws InterruptedException {
        String topic = TOPIC_PREFIX + "delay";
        String tag = TAG_PREFIX + "delay";
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> receivedMessages = new ArrayList<>();
        AtomicLong receiveTime = new AtomicLong(0);

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, topic, tag, message -> {
            receivedMessages.add(message);
            receiveTime.set(System.currentTimeMillis());
            latch.countDown();
        });

        // 等待消费者启动完成
        Thread.sleep(2000);

        // 发送延迟消息，延迟5秒
        TestEvent event = new TestEvent();
        event.setMessage("延迟消息测试");
        event.setTopic(topic);
        event.setTag(tag);
        event.setSendSuccessTime(new AtomicLong(0));
        log.info("发送延迟消息时间：{}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
        producer.asyncSendDelay(MQTypeEnum.ROCKET_MQ, topic, tag, event, 5);

        // 等待接收消息
        assertTrue(latch.await(30, TimeUnit.SECONDS), "消息接收超时");
        assertTrue(!receivedMessages.isEmpty(), "未收到消息");

        // 验证消息内容
        String receivedMessage = receivedMessages.get(0);
        com.alibaba.fastjson.JSONObject receivedJson = com.alibaba.fastjson.JSON.parseObject(receivedMessage);
        receivedJson.remove("sendSuccessTime");
        com.alibaba.fastjson.JSONObject expectedJson = com.alibaba.fastjson.JSON.parseObject(
            String.format("{\"message\":\"%s\",\"tag\":\"%s\",\"topic\":\"%s\"}",
                "延迟消息测试", tag, topic));
        assertTrue(receivedJson.equals(expectedJson),
            String.format("消息内容不匹配，期望：%s，实际：%s", expectedJson.toString(), receivedJson.toString()));

        // 验证延迟时间（由于RocketMQ使用预设的延迟级别，这里验证延迟时间是否在合理范围内）
        long delayTime = receiveTime.get() - event.getSendSuccessTime().get();
        log.info("消息延迟时间：{}秒", delayTime / 1000.0);
        // 由于5秒的延迟使用level 2（5秒延迟级别），所以延迟时间应该在4-7秒之间
        assertTrue(delayTime >= 4000 && delayTime <= 7000,
            String.format("延迟时间不在预期范围内：%.1f秒", delayTime / 1000.0));
    }

    @Test
    public void testMessageOrder() throws InterruptedException {
        String topic = TOPIC_PREFIX + "order";
        String tag = TAG_PREFIX + "order";
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(3);
        List<String> receivedMessages = new ArrayList<>();

        // 订阅消息
        mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, topic, tag, message -> {
            receivedMessages.add(message);
            latch.countDown();
        });

        // 等待消费者启动完成
        Thread.sleep(2000);

        // 发送三条顺序消息
        for (int i = 1; i <= 3; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("顺序消息测试" + i);
            event.setTopic(topic);
            event.setTag(tag);
            event.setSendSuccessTime(new AtomicLong(0));
            producer.syncSend(MQTypeEnum.ROCKET_MQ, topic, tag, event);
        }

        // 等待接收消息
        assertTrue(latch.await(30, TimeUnit.SECONDS), "消息接收超时");
        assertTrue(receivedMessages.size() == 3, "未收到全部消息");

        // 验证消息顺序
        for (int i = 0; i < 3; i++) {
            String receivedMessage = receivedMessages.get(i);
            com.alibaba.fastjson.JSONObject receivedJson = com.alibaba.fastjson.JSON.parseObject(receivedMessage);
            receivedJson.remove("sendSuccessTime");
            assertTrue(receivedJson.getString("message").contains("顺序消息测试" + (i + 1)), "消息顺序不正确");
        }
    }
}