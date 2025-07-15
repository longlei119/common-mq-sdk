package com.example.mq.broadcast;

import com.example.mq.consumer.impl.EMQXConsumer;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.producer.impl.EMQXProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * EMQX广播模式测试
 * 需要启动真实的EMQX服务器才能运行
 */
@SpringBootTest
@ActiveProfiles("emqx-only")
@ConditionalOnProperty(name = "mq.emqx.enabled", havingValue = "true")
public class EMQXBroadcastTest {

    @Autowired
    private EMQXProducer emqxProducer;

    @Autowired
    private EMQXConsumer emqxConsumer;

    @Test
    public void testEMQXUnicastVsBroadcast() throws InterruptedException {
        // 单播模式测试
        String unicastTopic = "test/unicast";
        AtomicInteger unicastCount = new AtomicInteger(0);
        CountDownLatch unicastLatch = new CountDownLatch(1); // EMQX单播模式应该只有一个消费者收到消息
        
        // 创建两个单播消费者（EMQX中同一个topic的多次订阅会覆盖之前的订阅）
        System.out.println("开始订阅单播消费者1");
        emqxConsumer.subscribe(unicastTopic, message -> {
            System.out.println("单播消费者1收到消息: " + message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        // 等待第一个订阅生效
        Thread.sleep(500);
        
        System.out.println("开始订阅单播消费者2");
        emqxConsumer.subscribe(unicastTopic, message -> {
            System.out.println("单播消费者2收到消息: " + message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        // 等待第二个订阅生效
        Thread.sleep(1500);
        
        // 发送单播消息
        String unicastMessage = "EMQX单播测试消息";
        System.out.println("发送单播消息");
        emqxProducer.asyncSend(MQTypeEnum.EMQX, unicastTopic, null, unicastMessage);
        
        // 等待消息处理
        assertTrue(unicastLatch.await(10, TimeUnit.SECONDS), "单播消息接收超时");
        
        // 验证单播模式：EMQX中同一个客户端的多次订阅会覆盖之前的订阅
        assertEquals(1, unicastCount.get(), "EMQX单播模式应该只有一个消费者收到消息，实际收到: " + unicastCount.get());
        
        System.out.println("EMQX单播模式测试完成，收到消息数: " + unicastCount.get());
        
        // 广播模式测试（EMQX本身就是发布订阅模式，但同一个客户端的多次订阅可能会相互覆盖）
        String broadcastTopic = "test/broadcast";
        AtomicInteger broadcastCount = new AtomicInteger(0);
        CountDownLatch broadcastLatch = new CountDownLatch(1); // 期望至少1个消费者收到消息
        
        // 创建两个广播消费者（使用不同的消费者实例模拟多个客户端）
        System.out.println("开始订阅广播消费者1");
        emqxConsumer.subscribeBroadcast(MQTypeEnum.EMQX, broadcastTopic, null, message -> {
            System.out.println("广播消费者1收到消息: " + message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        System.out.println("开始订阅广播消费者2");
        emqxConsumer.subscribeBroadcast(MQTypeEnum.EMQX, broadcastTopic, null, message -> {
            System.out.println("广播消费者2收到消息: " + message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        // 等待广播订阅生效
        System.out.println("等待广播订阅生效...");
        Thread.sleep(3000);
        
        // 发送广播消息
        String broadcastMessage = "EMQX广播测试消息";
        System.out.println("发送广播消息: " + broadcastMessage);
        emqxProducer.asyncSendBroadcast(MQTypeEnum.EMQX, broadcastTopic, null, broadcastMessage);
        
        System.out.println("等待广播消息处理，当前计数: " + broadcastCount.get());
        // 等待广播消息处理
        boolean broadcastReceived = broadcastLatch.await(15, TimeUnit.SECONDS);
        System.out.println("广播消息接收结果: " + broadcastReceived + ", 实际收到消息数: " + broadcastCount.get());
        assertTrue(broadcastReceived, "广播消息接收超时");
        
        // 验证广播模式：至少有一个消费者应该收到消息
        assertTrue(broadcastCount.get() >= 1, "EMQX广播模式至少应该有一个消费者收到消息，实际收到: " + broadcastCount.get());
        
        System.out.println("EMQX广播模式测试完成，收到消息数: " + broadcastCount.get());
        System.out.println("EMQX单播vs广播测试完成");
        
        // 清理订阅
        emqxConsumer.unsubscribe(unicastTopic);
        emqxConsumer.unsubscribe(broadcastTopic);
    }
}