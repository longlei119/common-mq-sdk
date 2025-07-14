package com.example.mq;

import com.example.mq.consumer.impl.EMQXConsumer;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;
import com.example.mq.model.TestEvent;
import com.example.mq.producer.impl.EMQXProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * EMQX真实环境测试
 * 需要启动真实的EMQX服务器才能运行
 */
@SpringBootTest
@ActiveProfiles("emqx-only")
@ConditionalOnProperty(name = "mq.emqx.enabled", havingValue = "true")
public class EMQXRealTest {

    @Autowired
    private EMQXProducer emqxProducer;

    @Autowired
    private EMQXConsumer emqxConsumer;

    @Test
    public void testSendAndReceiveMessage() throws InterruptedException {
        String topic = "test/topic";
        String testMessage = "Hello EMQX!";
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        
        // 订阅主题
        emqxConsumer.subscribe(topic, message -> {
            receivedMessage.set(message);
            latch.countDown();
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送消息
        emqxProducer.asyncSend(MQTypeEnum.EMQX, topic, null, testMessage);
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(testMessage, receivedMessage.get(), "接收到的消息内容不匹配");
        
        // 取消订阅
        emqxConsumer.unsubscribe(topic);
    }
    
    @Test
    public void testSendMQEvent() throws InterruptedException {
        String topic = "test/event";
        TestEvent testEvent = new TestEvent("Test event data");
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        
        // 订阅主题
        emqxConsumer.subscribe(topic, message -> {
            receivedMessage.set(message);
            latch.countDown();
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送MQEvent
        emqxProducer.syncSend(MQTypeEnum.EMQX, topic, null, testEvent);
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "消息接收超时");
        assertNotNull(receivedMessage.get(), "未接收到消息");
        assertTrue(receivedMessage.get().contains("TEST_EVENT"), "消息内容不包含事件类型");
        
        // 取消订阅
        emqxConsumer.unsubscribe(topic);
    }
    
    @Test
    public void testDelayMessage() throws InterruptedException {
        String topic = "test/delay";
        String testMessage = "Delay message test";
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        long startTime = System.currentTimeMillis();
        
        // 订阅主题
        emqxConsumer.subscribe(topic, message -> {
            receivedMessage.set(message);
            latch.countDown();
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送延迟消息（EMQX不支持原生延迟，通过DelayMessageSender实现）
        emqxProducer.asyncSendDelay(MQTypeEnum.EMQX, topic, null, testMessage, 5);
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(testMessage, receivedMessage.get(), "接收到的消息内容不匹配");
        
        long elapsedTime = System.currentTimeMillis() - startTime;
        // 由于EMQX不支持延迟消息，消息应该立即到达
        assertTrue(elapsedTime < 3000, "消息到达时间过长，应该立即发送");
        
        // 取消订阅
        emqxConsumer.unsubscribe(topic);
    }
    
    @Test
    public void testMultipleTopics() throws InterruptedException {
        String topic1 = "test/topic1";
        String topic2 = "test/topic2";
        String message1 = "Message for topic 1";
        String message2 = "Message for topic 2";
        
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<String> receivedMessage1 = new AtomicReference<>();
        AtomicReference<String> receivedMessage2 = new AtomicReference<>();
        
        // 订阅多个主题
        emqxConsumer.subscribe(topic1, message -> {
            receivedMessage1.set(message);
            latch.countDown();
        });
        
        emqxConsumer.subscribe(topic2, message -> {
            receivedMessage2.set(message);
            latch.countDown();
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送消息到不同主题
        emqxProducer.asyncSend(MQTypeEnum.EMQX, topic1, null, message1);
        emqxProducer.asyncSend(MQTypeEnum.EMQX, topic2, null, message2);
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(message1, receivedMessage1.get(), "主题1的消息内容不匹配");
        assertEquals(message2, receivedMessage2.get(), "主题2的消息内容不匹配");
        
        // 取消订阅
        emqxConsumer.unsubscribe(topic1);
        emqxConsumer.unsubscribe(topic2);
    }
}