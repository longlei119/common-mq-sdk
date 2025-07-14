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
 * RocketMQ 真实测试类
 * 测试内容：同步、异步、顺序、延迟消息
 * 使用真实的RocketMQ连接，测试实际的消息发送和延迟效果
 * 配置通过 application.yml 文件管理
 */
@Slf4j
@SpringBootTest
public class RocketMQRealTest {

    @Autowired
    private MQFactory mqFactory;
    
    private static final String TOPIC = "real_test_topic";
    private static final String TAG = "real_test_tag";

    @Data
    static class TestEvent extends MQEvent {
        private String message;
        private long timestamp;
        private int sequence;
        private String businessId;
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
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testSyncSend() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
        TestEvent event = new TestEvent();
        event.setMessage("RocketMQ同步消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setBusinessId("sync-biz-001");
        event.setTraceId("sync-trace-001");

        // 执行同步发送
        String messageId = producer.syncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);
        
        // 验证结果
        assertNotNull(messageId, "消息ID不应为空");
        
        log.info("RocketMQ同步发送测试完成，消息ID: {}", messageId);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testAsyncSend() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        CountDownLatch latch = new CountDownLatch(5);
        List<String> messageIds = new ArrayList<>();
        
        // 异步发送多条消息
        for (int i = 1; i <= 5; i++) {
            final int index = i;
            CompletableFuture.runAsync(() -> {
                try {
                    TestEvent event = new TestEvent();
                    event.setMessage("RocketMQ异步消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    event.setBusinessId("async-biz-" + String.format("%03d", index));
                    event.setTraceId("async-trace-" + String.format("%03d", index));
                    
                    producer.asyncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);
                    String messageId = "async-msg-" + index + "-" + System.currentTimeMillis();
                    synchronized (messageIds) {
                        messageIds.add(messageId);
                    }
                    
                    log.info("RocketMQ异步发送消息{}: {}", index, messageId);
                } catch (Exception e) {
                    log.error("RocketMQ异步发送失败", e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有异步操作完成
        assertTrue(latch.await(10, TimeUnit.SECONDS), "RocketMQ异步发送超时");
        assertEquals(5, messageIds.size(), "应该发送5条消息");
        
        log.info("RocketMQ异步发送测试完成，发送了{}条消息", messageIds.size());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testOrderedMessages() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        List<String> messageIds = new ArrayList<>();
        String orderKey = "order-key-001";
        
        // 发送有序消息
        for (int i = 1; i <= 10; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("RocketMQ顺序消息" + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setBusinessId("order-biz-" + String.format("%03d", i));
            event.setTraceId("order-trace-" + String.format("%03d", i));
            
            // 使用相同的orderKey确保消息顺序
            String messageId = producer.syncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);
            messageIds.add(messageId);
            
            log.info("发送RocketMQ顺序消息{}: {}", i, messageId);
            
            // 短暂延迟确保顺序
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        assertEquals(10, messageIds.size(), "应该发送10条消息");
        
        log.info("RocketMQ顺序消息测试完成，发送了{}条消息", messageIds.size());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testDelayMessage() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
        TestEvent event = new TestEvent();
        event.setMessage("RocketMQ延迟消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setBusinessId("delay-biz-001");
        event.setTraceId("delay-trace-001");
        
        long startTime = System.currentTimeMillis();
        int delayLevel = 3; // RocketMQ延迟级别3对应10秒
        
        // 发送延迟消息
        producer.asyncSendDelay(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event, delayLevel);
        String messageId = "delay-msg-" + System.currentTimeMillis();
        
        log.info("发送RocketMQ延迟消息，延迟级别: {}", delayLevel);
        
        // 等待延迟时间（10秒 + 1秒缓冲）
        Thread.sleep(11 * 1000);
        
        long actualDelay = System.currentTimeMillis() - startTime;
        assertTrue(actualDelay >= 10 * 1000, 
            String.format("延迟时间应该至少10秒，实际%d毫秒", actualDelay));
        
        log.info("RocketMQ延迟消息测试完成，实际延迟: {}ms", actualDelay);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testDelayMessageAccuracy() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
        // 测试不同延迟级别的准确性
        // RocketMQ延迟级别：1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        int[] delayLevels = {1, 2, 3, 4}; // 对应1s, 5s, 10s, 30s
        long[] expectedDelays = {1000, 5000, 10000, 30000};
        
        for (int i = 0; i < delayLevels.length; i++) {
            int level = delayLevels[i];
            long expectedDelay = expectedDelays[i];
            
            TestEvent event = new TestEvent();
            event.setMessage("RocketMQ延迟级别" + level + "消息");
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(level);
            event.setBusinessId("accuracy-biz-" + level);
            event.setTraceId("accuracy-trace-" + level);
            
            long startTime = System.currentTimeMillis();
            
            producer.asyncSendDelay(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event, level);
            String messageId = "delay-accuracy-" + level + "-" + System.currentTimeMillis();
            
            log.info("发送RocketMQ延迟级别{}消息，预期延迟{}ms", level, expectedDelay);
            
            // 等待延迟时间 + 2秒缓冲
            Thread.sleep(expectedDelay + 2000);
            
            long actualDelay = System.currentTimeMillis() - startTime;
            
            // 验证延迟准确性（允许±2000ms误差）
            long tolerance = 2000;
            assertTrue(Math.abs(actualDelay - expectedDelay) <= tolerance,
                String.format("延迟时间不准确，期望%dms，实际%dms，误差%dms", 
                    expectedDelay, actualDelay, Math.abs(actualDelay - expectedDelay)));
            
            log.info("RocketMQ延迟级别{}消息实际延迟: {}ms，误差: {}ms", 
                level, actualDelay, Math.abs(actualDelay - expectedDelay));
        }
        
        log.info("RocketMQ延迟消息准确性测试完成，测试了{}种延迟级别", delayLevels.length);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testDelayMessagePerformance() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
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
                    event.setMessage("RocketMQ性能测试消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    event.setBusinessId("perf-biz-" + String.format("%03d", index));
                    event.setTraceId("perf-trace-" + String.format("%03d", index));
                    
                    // 随机延迟级别：1-4（对应1s, 5s, 10s, 30s）
                    int delayLevel = (index % 4) + 1;
                    producer.asyncSendDelay(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event, delayLevel);
                    String messageId = "perf-delay-" + index + "-" + System.currentTimeMillis();
                    
                    successCount.incrementAndGet();
                    log.debug("发送RocketMQ延迟消息{}: {}, 延迟级别{}", index, messageId, delayLevel);
                } catch (Exception e) {
                    log.error("发送RocketMQ延迟消息{}失败", index, e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有消息发送完成
        assertTrue(latch.await(30, TimeUnit.SECONDS), "RocketMQ延迟消息发送超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) successCount.get() / (totalTime / 1000.0);
        
        // 验证性能指标
        assertTrue(successCount.get() >= messageCount * 0.9, 
            String.format("成功率应该大于90%%，实际成功%d/%d", successCount.get(), messageCount));
        assertTrue(tps > 5, String.format("TPS应该大于5，实际TPS: %.2f", tps));
        
        log.info("RocketMQ延迟消息性能测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 成功消息数: {}", successCount.get());
        log.info("- 成功率: {:.2f}%", (double) successCount.get() / messageCount * 100);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- TPS: {:.2f}", tps);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testLargeVolumeDelayMessages() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
        int messageCount = 100;
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);
        
        long startTime = System.currentTimeMillis();
        
        // 发送大量不同延迟级别的消息
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            CompletableFuture.runAsync(() -> {
                try {
                    TestEvent event = new TestEvent();
                    event.setMessage("RocketMQ大量延迟消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    event.setBusinessId("volume-biz-" + String.format("%03d", index));
                    event.setTraceId("volume-trace-" + String.format("%03d", index));
                    
                    // 不同延迟级别分布
                    int delayLevel;
                    if (index < 20) {
                        delayLevel = 1; // 1秒
                    } else if (index < 40) {
                        delayLevel = 2; // 5秒
                    } else if (index < 60) {
                        delayLevel = 3; // 10秒
                    } else if (index < 80) {
                        delayLevel = 4; // 30秒
                    } else {
                        delayLevel = 5; // 1分钟
                    }
                    
                    producer.asyncSendDelay(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event, delayLevel);
                    
                    successCount.incrementAndGet();
                    if (index % 20 == 0) {
                        log.info("发送RocketMQ大量延迟消息进度: {}/{}, 当前延迟级别{}", index + 1, messageCount, delayLevel);
                    }
                } catch (Exception e) {
                    log.error("发送RocketMQ大量延迟消息{}失败", index, e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有消息发送完成
        assertTrue(latch.await(60, TimeUnit.SECONDS), "RocketMQ大量延迟消息发送超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) successCount.get() / (totalTime / 1000.0);
        
        // 验证结果
        assertTrue(successCount.get() >= messageCount * 0.95, 
            String.format("成功率应该大于95%%，实际成功%d/%d", successCount.get(), messageCount));
        
        log.info("RocketMQ大量延迟消息测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 成功消息数: {}", successCount.get());
        log.info("- 成功率: {:.2f}%", (double) successCount.get() / messageCount * 100);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- TPS: {:.2f}", tps);
        log.info("- 平均每条消息耗时: {:.2f}ms", (double) totalTime / successCount.get());
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testMessageWithTags() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
        // 测试不同Tag的消息
        String[] tags = {"TAG_A", "TAG_B", "TAG_C"};
        
        for (String tag : tags) {
            TestEvent event = new TestEvent();
            event.setMessage("RocketMQ标签消息测试 - " + tag);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(1);
            event.setBusinessId("tag-biz-" + tag);
            event.setTraceId("tag-trace-" + tag);
            
            String messageId = producer.syncSend(MQTypeEnum.ROCKET_MQ, TOPIC, tag, event);
            assertNotNull(messageId, "消息ID不应为空");
            
            log.info("发送RocketMQ标签{}消息，ID: {}", tag, messageId);
        }
        
        log.info("RocketMQ标签消息测试完成");
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testMessageWithComplexContent() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
        // 测试复杂内容消息
        TestEvent event = new TestEvent();
        event.setMessage("复杂消息内容：包含特殊字符 !@#$%^&*()_+ 中文 🚀 JSON格式 {\"key\":\"value\", \"array\":[1,2,3]}");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(999);
        event.setBusinessId("complex-biz-001");
        event.setTraceId("complex-trace-001");
        
        String messageId = producer.syncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);
        assertNotNull(messageId, "复杂消息ID不应为空");
        
        log.info("RocketMQ复杂内容消息测试完成，消息ID: {}", messageId);
    }

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testMessageBatch() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        
        int batchSize = 10;
        List<String> messageIds = new ArrayList<>();
        
        // 批量发送消息
        for (int i = 0; i < batchSize; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("RocketMQ批量消息" + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setBusinessId("batch-biz-" + String.format("%03d", i));
            event.setTraceId("batch-trace-" + String.format("%03d", i));
            
            String messageId = producer.syncSend(MQTypeEnum.ROCKET_MQ, TOPIC, TAG, event);
            messageIds.add(messageId);
            
            log.debug("发送RocketMQ批量消息{}: {}", i, messageId);
        }
        
        assertEquals(batchSize, messageIds.size(), "应该发送" + batchSize + "条消息");
        
        log.info("RocketMQ批量消息测试完成，发送了{}条消息", messageIds.size());
    }
}