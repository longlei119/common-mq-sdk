package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.EMQXConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.model.TestEvent;
import com.lachesis.windrangerms.mq.producer.impl.EMQXProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * EMQX真实环境测试
 * 需要启动真实的EMQX服务器才能运行
 */
@SpringBootTest
// 使用默认统一配置
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
    public void testUnicastMode() throws InterruptedException {
        // 单播模式验证：多个消费者只有一个能收到消息
        String unicastTopic = "test/unicast/verify";
        
        AtomicInteger totalReceivedCount = new AtomicInteger(0);
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1); // 期望只有1个消费者收到消息
        
        // 创建第一个消费者
        emqxConsumer.subscribe(unicastTopic, message -> {
            consumer1Count.incrementAndGet();
            totalReceivedCount.incrementAndGet();
            latch.countDown();
        });
        
        // 创建第二个消费者（EMQX中同一个主题的多个消费者会负载均衡）
        emqxConsumer.subscribe(unicastTopic, message -> {
            consumer2Count.incrementAndGet();
            totalReceivedCount.incrementAndGet();
            latch.countDown();
        });
        
        // 等待消费者准备就绪
        Thread.sleep(2000);
        
        // 发送单播消息
        TestEvent event = new TestEvent("EMQX单播模式验证消息");
        
        emqxProducer.asyncSend(MQTypeEnum.EMQX, unicastTopic, null, event);
        
        // 等待消息处理
        assertTrue(latch.await(10, TimeUnit.SECONDS), "单播消息接收超时");
        
        // 验证单播特性：只有一个消费者收到消息（EMQX负载均衡）
        assertEquals(1, totalReceivedCount.get(), 
            "EMQX单播模式应该只有一个消费者收到消息，实际收到总数: " + totalReceivedCount.get());
        
        // EMQX中多个消费者会负载均衡，所以只有一个消费者收到消息
        assertTrue(consumer1Count.get() + consumer2Count.get() == 1, 
            "总共应该只有一个消费者收到消息");
        assertTrue((consumer1Count.get() == 1 && consumer2Count.get() == 0) || 
                  (consumer1Count.get() == 0 && consumer2Count.get() == 1), 
            "应该只有一个消费者收到消息");
        
        // 清理资源
        emqxConsumer.unsubscribe(unicastTopic);
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
        
        // 发送延迟消息（通过DelayMessageSender实现）
        emqxProducer.asyncSendDelay(MQTypeEnum.EMQX, topic, null, testMessage, 3);
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(testMessage, receivedMessage.get(), "接收到的消息内容不匹配");
        
        long elapsedTime = System.currentTimeMillis() - startTime;
        // 延迟消息应该在指定时间后到达，允许一定误差
        assertTrue(elapsedTime >= 2000, "消息到达时间过短，延迟功能未生效");
        assertTrue(elapsedTime <= 6000, "消息到达时间过长，延迟时间不准确");
        
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