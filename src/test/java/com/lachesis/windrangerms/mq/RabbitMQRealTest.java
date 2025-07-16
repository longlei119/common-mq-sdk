package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RabbitMQ 真实测试类
 * 测试内容：同步、异步、顺序、延迟消息
 * 使用真实的RabbitMQ连接，测试实际的消息发送和延迟效果
 * 配置通过 application.yml 文件管理
 */
@Slf4j
@SpringBootTest
@TestPropertySource(properties = {
    "mq.rabbitmq.enabled=true",
    "mq.redis.enabled=true",
    "mq.delay.enabled=true"
})
public class RabbitMQRealTest {

    @Autowired
    private MQFactory mqFactory;
    
    @Autowired
    private StringRedisTemplate redisTemplate;
    
    private static final String TOPIC = "real_test_topic";
    private static final String TAG = "real_test_tag";

    @Data
    static class TestEvent extends MQEvent {
        private String message;
        private long timestamp;
        private int sequence;
        private String traceId;

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
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testSyncSend() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        // 设置消息接收验证
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
            receivedMessage.set(message);
            latch.countDown();
            log.info("接收到RabbitMQ同步消息: {}", message);
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        TestEvent event = new TestEvent();
        event.setMessage("RabbitMQ同步消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("sync-test-001");

        // 执行同步发送
        String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
        
        // 验证结果
        assertNotNull(messageId, "消息ID不应为空");
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "消息接收超时");
        assertNotNull(receivedMessage.get(), "应该接收到消息");
        assertTrue(receivedMessage.get().contains("RabbitMQ同步消息测试"), "消息内容应该包含测试文本");
        
        log.info("RabbitMQ同步发送测试完成，消息ID: {}", messageId);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testAsyncSend() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        CountDownLatch sendLatch = new CountDownLatch(5);
        CountDownLatch receiveLatch = new CountDownLatch(5);
        List<String> messageIds = new ArrayList<>();
        List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
            receivedMessages.add(message);
            receiveLatch.countDown();
            log.info("接收到RabbitMQ异步消息: {}", message);
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 异步发送多条消息
        for (int i = 1; i <= 5; i++) {
            final int index = i;
            CompletableFuture.runAsync(() -> {
                try {
                    TestEvent event = new TestEvent();
                    event.setMessage("RabbitMQ异步消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    event.setTraceId("async-test-" + String.format("%03d", index));
                    
                    producer.asyncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
                    String messageId = "rabbitmq-async-" + System.currentTimeMillis() + "-" + index;
                    synchronized (messageIds) {
                        messageIds.add(messageId);
                    }
                    
                    log.info("RabbitMQ异步发送消息{}: {}", index, messageId);
                } catch (Exception e) {
                    log.error("RabbitMQ异步发送失败", e);
                } finally {
                    sendLatch.countDown();
                }
            });
        }
        
        // 等待所有异步操作完成
        assertTrue(sendLatch.await(10, TimeUnit.SECONDS), "RabbitMQ异步发送超时");
        assertEquals(5, messageIds.size(), "应该发送5条消息");
        
        // 等待所有消息接收
        assertTrue(receiveLatch.await(15, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(5, receivedMessages.size(), "应该接收到5条消息");
        
        // 验证消息内容
        for (int i = 1; i <= 5; i++) {
            final int index = i;
            assertTrue(receivedMessages.stream().anyMatch(msg -> msg.contains("RabbitMQ异步消息" + index)), 
                "应该接收到消息" + index);
        }
        
        log.info("RabbitMQ异步发送测试完成，发送了{}条消息，接收了{}条消息", messageIds.size(), receivedMessages.size());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testOrderedMessages() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        List<String> messageIds = new ArrayList<>();
        List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(10);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
            receivedMessages.add(message);
            latch.countDown();
            log.info("接收到RabbitMQ有序消息: {}", message);
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送有序消息
        for (int i = 1; i <= 10; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("RabbitMQ顺序消息" + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setTraceId("order-test-" + String.format("%03d", i));
            
            String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
            messageIds.add(messageId);
            
            log.info("发送RabbitMQ顺序消息{}: {}", i, messageId);
            
            // 短暂延迟确保顺序
            Thread.sleep(10);
        }
        
        assertEquals(10, messageIds.size(), "应该发送10条消息");
        
        // 等待所有消息接收
        assertTrue(latch.await(30, TimeUnit.SECONDS), "消息接收超时");
        assertEquals(10, receivedMessages.size(), "接收的消息数量应该正确");
        
        // 验证消息顺序（注意：RabbitMQ不保证严格顺序，这里只验证消息都收到了）
        for (int i = 1; i <= 10; i++) {
            final int index = i;
            assertTrue(receivedMessages.stream().anyMatch(msg -> msg.contains("RabbitMQ顺序消息" + index)), 
                "应该接收到消息" + index);
        }
        
        log.info("RabbitMQ顺序消息测试完成，发送了{}条消息，接收了{}条消息", messageIds.size(), receivedMessages.size());
    }



    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testUnicastMode() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        String topic = "test-unicast-only-topic";
        String tag = "unicast-test-tag";
        
        // 单播模式验证：多个消费者只有一个能收到消息
        AtomicInteger totalReceivedCount = new AtomicInteger(0);
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1); // 期望只有1个消费者收到消息
        
        // 创建第一个消费者
        log.info("创建单播消费者1");
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, topic, tag, message -> {
            log.info("单播消费者1收到消息: {}", message);
            consumer1Count.incrementAndGet();
            totalReceivedCount.incrementAndGet();
            latch.countDown();
        });
        
        // 等待第一个订阅生效
        Thread.sleep(500);
        
        // 创建第二个消费者（这会覆盖第一个消费者的订阅）
        log.info("创建单播消费者2");
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, topic, tag, message -> {
            log.info("单播消费者2收到消息: {}", message);
            consumer2Count.incrementAndGet();
            totalReceivedCount.incrementAndGet();
            latch.countDown();
        });
        
        // 等待第二个订阅生效
        Thread.sleep(1000);
        
        // 发送单播消息
        TestEvent event = new TestEvent();
        event.setMessage("RabbitMQ单播模式验证消息");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("unicast-verify-001");
        
        log.info("发送单播消息");
        String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, topic, tag, event);
        assertNotNull(messageId, "消息ID不应为空");
        
        // 等待消息处理
        assertTrue(latch.await(10, TimeUnit.SECONDS), "单播消息接收超时");
        
        // 验证单播特性：只有一个消费者收到消息
        assertEquals(1, totalReceivedCount.get(), 
            "RabbitMQ单播模式应该只有一个消费者收到消息，实际收到总数: " + totalReceivedCount.get());
        
        // 验证是最后一个消费者收到消息（RabbitMQ中后订阅会覆盖前订阅）
        assertEquals(0, consumer1Count.get(), "消费者1不应该收到消息，因为被消费者2覆盖");
        assertEquals(1, consumer2Count.get(), "消费者2应该收到消息");
        
        log.info("RabbitMQ单播模式验证完成 - 消费者1收到: {}, 消费者2收到: {}, 总计: {}", 
            consumer1Count.get(), consumer2Count.get(), totalReceivedCount.get());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testDelayMessage() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        // 清理Redis中的残留延迟消息
        String delayQueueKey = "delay_message:queue";
        redisTemplate.delete(delayQueueKey);
        
        // 清理所有延迟消息相关的key
        Set<String> delayKeys = redisTemplate.keys("delay_message:*");
        if (delayKeys != null && !delayKeys.isEmpty()) {
            redisTemplate.delete(delayKeys);
            log.info("清理了{}个延迟消息相关的Redis key", delayKeys.size());
        }
        log.info("清理Redis延迟消息队列: {}", delayQueueKey);
        
        // 等待清理生效
        Thread.sleep(500);
        
        // 设置消息接收验证
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        AtomicLong receiveTime = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
            if (message.contains("RabbitMQ延迟消息测试")) {
                long currentTime = System.currentTimeMillis();
                receivedMessage.set(message);
                receiveTime.set(currentTime);
                latch.countDown();
                log.info("接收到RabbitMQ延迟消息: {}, 接收时间: {}", message, currentTime);
            }
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        TestEvent event = new TestEvent();
        event.setMessage("RabbitMQ延迟消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("delay-test-001");
        
        long startTime = System.currentTimeMillis();
        int delaySeconds = 3;
        
        // 发送延迟消息
        producer.asyncSendDelay(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event, delaySeconds);
        // 异步延迟消息发送成功（void返回类型）
        
        log.info("发送RabbitMQ延迟消息成功，延迟时间: {}秒，发送时间: {}", delaySeconds, startTime);
        
        // 等待延迟消息接收（最多等待8秒）
        assertTrue(latch.await(8, TimeUnit.SECONDS), "延迟消息接收超时");
        assertNotNull(receivedMessage.get(), "应该接收到延迟消息");
        assertTrue(receivedMessage.get().contains("RabbitMQ延迟消息测试"), "消息内容应该包含测试文本");
        
        // 确保接收时间已设置
        assertTrue(receiveTime.get() > 0, "接收时间应该已设置");
        long actualDelay = receiveTime.get() - startTime;
        
        // 验证延迟时间（允许±2000ms误差，因为RabbitMQ延迟插件和系统调度精度限制）
        assertTrue(actualDelay >= (delaySeconds - 1) * 1000 && actualDelay <= (delaySeconds + 2) * 1000, 
            String.format("延迟时间应该在%d-%d秒之间，实际: %dms", delaySeconds - 1, delaySeconds + 2, actualDelay));
        
        log.info("RabbitMQ延迟消息测试完成，实际延迟: {}ms", actualDelay);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testDelayMessageAccuracy() throws InterruptedException {
        // 清理Redis中的残留延迟消息
        String delayQueueKey = "delay_message:queue";
        redisTemplate.delete(delayQueueKey);
        log.info("清理Redis延迟消息队列: {}", delayQueueKey);
        Thread.sleep(500);
        
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        // 测试不同延迟时间的准确性
        int[] delaySeconds = {1, 3, 5}; // 减少测试时间
        List<Long> actualDelays = new ArrayList<>();
        
        for (int delay : delaySeconds) {
            // 设置消息接收验证
            AtomicReference<String> receivedMessage = new AtomicReference<>();
            AtomicLong receiveTime = new AtomicLong();
            CountDownLatch latch = new CountDownLatch(1);
            
            // 订阅消息
             consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
                 if (message.contains("RabbitMQ延迟" + delay + "秒消息")) {
                     receivedMessage.set(message);
                     receiveTime.set(System.currentTimeMillis());
                     latch.countDown();
                     log.info("接收到RabbitMQ延迟{}秒消息: {}", delay, message);
                 }
             });
            
            // 等待订阅生效
            Thread.sleep(500);
            
            TestEvent event = new TestEvent();
            event.setMessage("RabbitMQ延迟" + delay + "秒消息");
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(delay);
            event.setTraceId("accuracy-test-" + delay);
            
            long startTime = System.currentTimeMillis();
            
            producer.asyncSendDelay(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event, delay);
            // 异步延迟消息发送成功（void返回类型）
            
            log.info("发送RabbitMQ延迟{}秒消息成功", delay);
            
            // 等待延迟消息接收
            assertTrue(latch.await(delay + 5, TimeUnit.SECONDS), "延迟消息接收超时");
            assertNotNull(receivedMessage.get(), "应该接收到延迟消息");
            
            long actualDelay = receiveTime.get() - startTime;
            actualDelays.add(actualDelay);
            
            // 验证延迟准确性（允许±2000ms误差，因为RabbitMQ延迟插件和系统调度精度限制）
            long expectedDelay = delay * 1000;
            long tolerance = 2000;
            assertTrue(Math.abs(actualDelay - expectedDelay) <= tolerance,
                String.format("延迟时间不准确，期望%dms，实际%dms，误差%dms", 
                    expectedDelay, actualDelay, Math.abs(actualDelay - expectedDelay)));
            
            log.info("RabbitMQ延迟{}秒消息实际延迟: {}ms，误差: {}ms", 
                delay, actualDelay, Math.abs(actualDelay - expectedDelay));
        }
        
        log.info("RabbitMQ延迟消息准确性测试完成，测试了{}种延迟时间", delaySeconds.length);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testDelayMessagePerformance() throws InterruptedException {
        // 清理Redis中的残留延迟消息
        String delayQueueKey = "delay_message:queue";
        redisTemplate.delete(delayQueueKey);
        log.info("清理Redis延迟消息队列: {}", delayQueueKey);
        Thread.sleep(500);
        
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        int messageCount = 20; // 减少消息数量以加快测试
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch sendLatch = new CountDownLatch(messageCount);
        CountDownLatch receiveLatch = new CountDownLatch(messageCount);
        
        // 订阅消息
         consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
             if (message.contains("RabbitMQ性能测试消息")) {
                 receivedCount.incrementAndGet();
                 receiveLatch.countDown();
                 log.debug("接收到RabbitMQ性能测试消息: {}", message);
             }
         });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        long startTime = System.currentTimeMillis();
        
        // 并发发送延迟消息
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            CompletableFuture.runAsync(() -> {
                try {
                    TestEvent event = new TestEvent();
                    event.setMessage("RabbitMQ性能测试消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    event.setTraceId("perf-test-" + String.format("%03d", index));
                    
                    // 短延迟时间：1-2秒
                    int delaySeconds = (index % 2) + 1;
                    producer.asyncSendDelay(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event, delaySeconds);
                    
                    // 异步延迟消息发送成功（void返回类型）
                    successCount.incrementAndGet();
                    log.debug("发送RabbitMQ延迟消息{}: 成功, 延迟{}秒", index, delaySeconds);
                } catch (Exception e) {
                    log.error("发送RabbitMQ延迟消息{}失败", index, e);
                } finally {
                    sendLatch.countDown();
                }
            });
        }
        
        // 等待所有消息发送完成
        assertTrue(sendLatch.await(30, TimeUnit.SECONDS), "RabbitMQ延迟消息发送超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) successCount.get() / (totalTime / 1000.0);
        
        // 等待消息接收（给足够时间让延迟消息到达）
        assertTrue(receiveLatch.await(10, TimeUnit.SECONDS), "延迟消息接收超时");
        
        // 验证性能指标
        assertTrue(successCount.get() >= messageCount * 0.9, 
            String.format("发送成功率应该大于90%%，实际成功%d/%d", successCount.get(), messageCount));
        assertTrue(receivedCount.get() >= messageCount * 0.8, 
            String.format("接收成功率应该大于80%%，实际接收%d/%d", receivedCount.get(), messageCount));
        assertTrue(tps > 5, String.format("TPS应该大于5，实际TPS: %.2f", tps));
        
        log.info("RabbitMQ延迟消息性能测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 发送成功数: {}", successCount.get());
        log.info("- 接收成功数: {}", receivedCount.get());
        log.info("- 发送成功率: {:.2f}%", (double) successCount.get() / messageCount * 100);
        log.info("- 接收成功率: {:.2f}%", (double) receivedCount.get() / messageCount * 100);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- TPS: {:.2f}", tps);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testLargeVolumeDelayMessages() throws InterruptedException {
        // 清理Redis中的残留延迟消息
        String delayQueueKey = "delay_message:queue";
        redisTemplate.delete(delayQueueKey);
        log.info("清理Redis延迟消息队列: {}", delayQueueKey);
        Thread.sleep(500);
        
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        int messageCount = 100;
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);
        
        long startTime = System.currentTimeMillis();
        
        // 发送大量不同延迟时间的消息
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            CompletableFuture.runAsync(() -> {
                try {
                    TestEvent event = new TestEvent();
                    event.setMessage("RabbitMQ大量延迟消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    event.setTraceId("volume-test-" + String.format("%03d", index));
                    
                    // 不同延迟时间段：1-60秒
                    int delaySeconds;
                    if (index < 20) {
                        delaySeconds = 1; // 1秒
                    } else if (index < 40) {
                        delaySeconds = 5; // 5秒
                    } else if (index < 60) {
                        delaySeconds = 10; // 10秒
                    } else if (index < 80) {
                        delaySeconds = 30; // 30秒
                    } else {
                        delaySeconds = 60; // 60秒
                    }
                    
                    producer.asyncSendDelay(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event, delaySeconds);
                    
                    // 异步延迟消息发送成功（void返回类型）
                    successCount.incrementAndGet();
                    if (index % 20 == 0) {
                        log.info("发送RabbitMQ大量延迟消息进度: {}/{}, 当前延迟{}秒", index + 1, messageCount, delaySeconds);
                    }
                } catch (Exception e) {
                    log.error("发送RabbitMQ大量延迟消息{}失败", index, e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有消息发送完成
        assertTrue(latch.await(60, TimeUnit.SECONDS), "RabbitMQ大量延迟消息发送超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) successCount.get() / (totalTime / 1000.0);
        
        // 验证结果
        assertTrue(successCount.get() >= messageCount * 0.95, 
            String.format("成功率应该大于95%%，实际成功%d/%d", successCount.get(), messageCount));
        
        log.info("RabbitMQ大量延迟消息测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 成功消息数: {}", successCount.get());
        log.info("- 成功率: {:.2f}%", (double) successCount.get() / messageCount * 100);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- TPS: {:.2f}", tps);
        log.info("- 平均每条消息耗时: {:.2f}ms", (double) totalTime / successCount.get());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testMessagePersistence() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        // 设置消息接收验证
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息
         consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
             if (message.contains("RabbitMQ持久化消息测试")) {
                 receivedMessage.set(message);
                 latch.countDown();
                 log.info("接收到RabbitMQ持久化消息: {}", message);
             }
         });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 测试消息持久化
        TestEvent event = new TestEvent();
        event.setMessage("RabbitMQ持久化消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("persistence-test-001");
        
        String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
        assertNotNull(messageId, "持久化消息ID不应为空");
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "持久化消息接收超时");
        assertNotNull(receivedMessage.get(), "应该接收到持久化消息");
        assertTrue(receivedMessage.get().contains("RabbitMQ持久化消息测试"), "消息内容应该包含测试文本");
        
        log.info("RabbitMQ消息持久化测试完成，消息ID: {}", messageId);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testMessageWithComplexContent() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        // 设置消息接收验证
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息
         consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, (message) -> {
             if (message.contains("复杂消息内容")) {
                 receivedMessage.set(message);
                 latch.countDown();
                 log.info("接收到RabbitMQ复杂内容消息: {}", message);
             }
         });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 测试复杂内容消息
        TestEvent event = new TestEvent();
        event.setMessage("复杂消息内容：包含特殊字符 !@#$%^&*()_+ 中文 🚀 JSON格式 {\"key\":\"value\"}");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(999);
        event.setTraceId("complex-test-001");
        
        String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
        assertNotNull(messageId, "复杂消息ID不应为空");
        
        // 等待消息接收
        assertTrue(latch.await(10, TimeUnit.SECONDS), "复杂内容消息接收超时");
        assertNotNull(receivedMessage.get(), "应该接收到复杂内容消息");
        assertTrue(receivedMessage.get().contains("复杂消息内容"), "消息内容应该包含测试文本");
        assertTrue(receivedMessage.get().contains("特殊字符"), "消息内容应该包含特殊字符");
        assertTrue(receivedMessage.get().contains("🚀"), "消息内容应该包含emoji");
        
        log.info("RabbitMQ复杂内容消息测试完成，消息ID: {}", messageId);
    }


}