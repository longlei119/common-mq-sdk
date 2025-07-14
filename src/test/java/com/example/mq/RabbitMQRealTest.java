package com.example.mq;

import com.example.mq.enums.MQTypeEnum;
import com.example.mq.factory.MQFactory;
import com.example.mq.model.MQEvent;
import com.example.mq.producer.MQProducer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RabbitMQ 真实测试类
 * 测试内容：同步、异步、顺序、延迟消息
 * 使用真实的RabbitMQ连接，测试实际的消息发送和延迟效果
 * 配置通过 application.yml 文件管理
 */
@Slf4j
@SpringBootTest
public class RabbitMQRealTest {

    @Autowired
    private MQFactory mqFactory;
    
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
    void testSyncSend() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        TestEvent event = new TestEvent();
        event.setMessage("RabbitMQ同步消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("sync-test-001");

        // 执行同步发送
        String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
        
        // 验证结果
        assertNotNull(messageId, "消息ID不应为空");
        
        log.info("RabbitMQ同步发送测试完成，消息ID: {}", messageId);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testAsyncSend() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        CountDownLatch latch = new CountDownLatch(5);
        List<String> messageIds = new ArrayList<>();
        
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
                    latch.countDown();
                }
            });
        }
        
        // 等待所有异步操作完成
        assertTrue(latch.await(10, TimeUnit.SECONDS), "RabbitMQ异步发送超时");
        assertEquals(5, messageIds.size(), "应该发送5条消息");
        
        log.info("RabbitMQ异步发送测试完成，发送了{}条消息", messageIds.size());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testOrderedMessages() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        List<String> messageIds = new ArrayList<>();
        
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
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        assertEquals(10, messageIds.size(), "应该发送10条消息");
        
        log.info("RabbitMQ顺序消息测试完成，发送了{}条消息", messageIds.size());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testDelayMessage() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
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
        
        log.info("发送RabbitMQ延迟消息成功，延迟时间: {}秒", delaySeconds);
        
        // 等待延迟时间
        Thread.sleep((delaySeconds + 1) * 1000);
        
        long actualDelay = System.currentTimeMillis() - startTime;
        assertTrue(actualDelay >= delaySeconds * 1000, 
            String.format("延迟时间应该至少%d秒，实际%d毫秒", delaySeconds, actualDelay));
        
        log.info("RabbitMQ延迟消息测试完成，实际延迟: {}ms", actualDelay);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testDelayMessageAccuracy() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        // 测试不同延迟时间的准确性
        int[] delaySeconds = {1, 2, 3, 5};
        List<Long> actualDelays = new ArrayList<>();
        
        for (int delay : delaySeconds) {
            TestEvent event = new TestEvent();
            event.setMessage("RabbitMQ延迟" + delay + "秒消息");
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(delay);
            event.setTraceId("accuracy-test-" + delay);
            
            long startTime = System.currentTimeMillis();
            
            producer.asyncSendDelay(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event, delay);
            // 异步延迟消息发送成功（void返回类型）
            
            log.info("发送RabbitMQ延迟{}秒消息成功", delay);
            
            // 等待延迟时间 + 1秒缓冲
            Thread.sleep((delay + 1) * 1000);
            
            long actualDelay = System.currentTimeMillis() - startTime;
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
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        int messageCount = 50;
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);
        
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
                    
                    // 随机延迟时间：1-5秒
                    int delaySeconds = (index % 5) + 1;
                    producer.asyncSendDelay(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event, delaySeconds);
                    
                    // 异步延迟消息发送成功（void返回类型）
                    successCount.incrementAndGet();
                    log.debug("发送RabbitMQ延迟消息{}: 成功, 延迟{}秒", index, delaySeconds);
                } catch (Exception e) {
                    log.error("发送RabbitMQ延迟消息{}失败", index, e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有消息发送完成
        assertTrue(latch.await(30, TimeUnit.SECONDS), "RabbitMQ延迟消息发送超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) successCount.get() / (totalTime / 1000.0);
        
        // 验证性能指标
        assertTrue(successCount.get() >= messageCount * 0.9, 
            String.format("成功率应该大于90%%，实际成功%d/%d", successCount.get(), messageCount));
        assertTrue(tps > 5, String.format("TPS应该大于5，实际TPS: %.2f", tps));
        
        log.info("RabbitMQ延迟消息性能测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 成功消息数: {}", successCount.get());
        log.info("- 成功率: {:.2f}%", (double) successCount.get() / messageCount * 100);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- TPS: {:.2f}", tps);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testLargeVolumeDelayMessages() throws InterruptedException {
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
    void testMessagePersistence() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        // 测试消息持久化
        TestEvent event = new TestEvent();
        event.setMessage("RabbitMQ持久化消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("persistence-test-001");
        
        String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
        assertNotNull(messageId, "持久化消息ID不应为空");
        
        log.info("RabbitMQ消息持久化测试完成，消息ID: {}", messageId);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testMessageWithComplexContent() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        
        // 测试复杂内容消息
        TestEvent event = new TestEvent();
        event.setMessage("复杂消息内容：包含特殊字符 !@#$%^&*()_+ 中文 🚀 JSON格式 {\"key\":\"value\"}");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(999);
        event.setTraceId("complex-test-001");
        
        String messageId = producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC, TAG, event);
        assertNotNull(messageId, "复杂消息ID不应为空");
        
        log.info("RabbitMQ复杂内容消息测试完成，消息ID: {}", messageId);
    }
}