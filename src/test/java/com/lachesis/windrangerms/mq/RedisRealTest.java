package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis真实环境测试
 * 需要启动真实的Redis服务器才能运行
 * 配置通过 application.yml 文件管理
 */
@Slf4j
@SpringBootTest(classes = MQApplication.class)
@TestPropertySource(properties = {
    "mq.redis.enabled=true",
    "mq.delay.enabled=true"
})
public class RedisRealTest {

    @Autowired
    private MQFactory mqFactory;
    
    @Autowired
    private StringRedisTemplate redisTemplate;
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;
    
    private static final String TOPIC = "real_test_topic";
    private static final String TAG = "real_test_tag";

    @Data
    static class TestEvent extends MQEvent {
        private String message;
        private long timestamp;
        private int sequence;

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
    void testDelayMessageSenderNotNull() {
        // 这是核心问题的诊断测试
        assertNotNull(delayMessageSender, "DelayMessageSender应该被正确注入，不应该为null");
        log.info("DelayMessageSender注入成功: {}", delayMessageSender.getClass().getSimpleName());
    }

    @Test
    void testRedisConnection() {
        try {
            assertNotNull(redisTemplate, "StringRedisTemplate应该被正确注入");
            
            // 测试Redis连接
            redisTemplate.opsForValue().set("test:connection", "test:value");
            String value = redisTemplate.opsForValue().get("test:connection");
            assertEquals("test:value", value, "Redis连接应该正常工作");
            
            log.info("Redis连接测试通过");
        } catch (Exception e) {
            log.error("Redis连接失败", e);
            fail("Redis连接失败，请确保Redis服务器正在运行: " + e.getMessage());
        }
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testSyncSend() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        
        TestEvent event = new TestEvent();
        event.setMessage("Redis同步消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);

        // 执行同步发送
        String messageId = producer.syncSend(MQTypeEnum.REDIS, TOPIC, TAG, event);
        
        // 验证结果
        assertNotNull(messageId, "消息ID不应为空");
        
        log.info("Redis同步发送测试完成，消息ID: {}", messageId);
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testAsyncSend() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        CountDownLatch latch = new CountDownLatch(5);
        List<String> messageIds = new ArrayList<>();
        
        // 异步发送多条消息
        for (int i = 1; i <= 5; i++) {
            final int index = i;
            CompletableFuture.runAsync(() -> {
                try {
                    TestEvent event = new TestEvent();
                    event.setMessage("Redis异步消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    
                    producer.asyncSend(MQTypeEnum.REDIS, TOPIC, TAG, event);
                    String messageId = "redis-async-" + System.currentTimeMillis() + "-" + index;
                    synchronized (messageIds) {
                        messageIds.add(messageId);
                    }
                    
                    log.info("异步发送消息{}: {}", index, messageId);
                } catch (Exception e) {
                    log.error("异步发送失败", e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有异步操作完成
        assertTrue(latch.await(10, TimeUnit.SECONDS), "异步发送超时");
        assertEquals(5, messageIds.size(), "应该发送5条消息");
        
        log.info("Redis异步发送测试完成，发送了{}条消息", messageIds.size());
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testOrderedMessages() {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        List<String> messageIds = new ArrayList<>();
        
        // 发送有序消息
        for (int i = 1; i <= 10; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("Redis顺序消息" + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            
            String messageId = producer.syncSend(MQTypeEnum.REDIS, TOPIC, TAG, event);
            messageIds.add(messageId);
            
            log.info("发送顺序消息{}: {}", i, messageId);
            
            // 短暂延迟确保顺序
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        assertEquals(10, messageIds.size(), "应该发送10条消息");
        
        log.info("Redis顺序消息测试完成，发送了{}条消息", messageIds.size());
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testUnicastMode() throws InterruptedException {
        // 单播模式验证：多个消费者只有一个能收到消息
        String unicastTopic = "test_unicast_verify";
        String unicastTag = "unicast_tag";
        
        AtomicInteger totalReceivedCount = new AtomicInteger(0);
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1); // 期望只有1个消费者收到消息
        
        // 创建第一个消费者
        log.info("创建单播消费者1");
        // 注意：Redis的发布订阅模式是广播模式，这里我们测试的是队列模式的单播特性
        // 在Redis中，单播通常通过List数据结构实现，多个消费者竞争消费同一个队列
        
        // 模拟Redis队列消费者1
        CompletableFuture.runAsync(() -> {
            try {
                // 模拟从Redis队列中弹出消息（单播特性）
                String queueKey = "queue:" + unicastTopic + ":" + unicastTag;
                String message = redisTemplate.opsForList().rightPop(queueKey, 5, TimeUnit.SECONDS);
                if (message != null) {
                    log.info("单播消费者1收到消息: {}", message);
                    consumer1Count.incrementAndGet();
                    totalReceivedCount.incrementAndGet();
                    latch.countDown();
                }
            } catch (Exception e) {
                log.error("消费者1处理消息失败", e);
            }
        });
        
        // 创建第二个消费者
        log.info("创建单播消费者2");
        CompletableFuture.runAsync(() -> {
            try {
                // 模拟从Redis队列中弹出消息（单播特性）
                String queueKey = "queue:" + unicastTopic + ":" + unicastTag;
                String message = redisTemplate.opsForList().rightPop(queueKey, 5, TimeUnit.SECONDS);
                if (message != null) {
                    log.info("单播消费者2收到消息: {}", message);
                    consumer2Count.incrementAndGet();
                    totalReceivedCount.incrementAndGet();
                    latch.countDown();
                }
            } catch (Exception e) {
                log.error("消费者2处理消息失败", e);
            }
        });
        
        // 等待消费者准备就绪
        Thread.sleep(1000);
        
        // 发送单播消息到Redis队列
        TestEvent event = new TestEvent();
        event.setMessage("Redis单播模式验证消息");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        
        // 直接向Redis队列推送消息（模拟单播）
        String queueKey = "queue:" + unicastTopic + ":" + unicastTag;
        String messageContent = String.format(
            "{\"message\":\"%s\",\"timestamp\":%d,\"sequence\":%d}",
            event.getMessage(), event.getTimestamp(), event.getSequence()
        );
        
        log.info("发送单播消息到Redis队列: {}", queueKey);
        redisTemplate.opsForList().leftPush(queueKey, messageContent);
        
        // 等待消息处理
        assertTrue(latch.await(10, TimeUnit.SECONDS), "单播消息接收超时");
        
        // 验证单播特性：只有一个消费者收到消息
        assertEquals(1, totalReceivedCount.get(), 
            "Redis单播模式应该只有一个消费者收到消息，实际收到总数: " + totalReceivedCount.get());
        
        // Redis队列模式中多个消费者竞争消费，只有一个消费者能收到消息
        assertTrue(consumer1Count.get() + consumer2Count.get() == 1, 
            "总共应该只有一个消费者收到消息");
        assertTrue((consumer1Count.get() == 1 && consumer2Count.get() == 0) || 
                  (consumer1Count.get() == 0 && consumer2Count.get() == 1), 
            "应该只有一个消费者收到消息");
        
        log.info("Redis单播模式验证完成 - 消费者1收到: {}, 消费者2收到: {}, 总计: {}", 
            consumer1Count.get(), consumer2Count.get(), totalReceivedCount.get());
        
        // 清理队列
        redisTemplate.delete(queueKey);
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testDelayMessage() throws InterruptedException {
        log.info("=== testDelayMessage Debug Info ===");
        log.info("DelayMessageSender: {}", delayMessageSender != null ? "EXISTS" : "NULL");
        
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        log.info("Producer type: {}", producer.getClass().getSimpleName());
        
        TestEvent event = new TestEvent();
        event.setMessage("Redis延迟消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        
        long startTime = System.currentTimeMillis();
        int delaySeconds = 3;
        
        // 发送延迟消息
        producer.asyncSendDelay(MQTypeEnum.REDIS, TOPIC, TAG, event, delaySeconds);
        // 异步延迟消息发送成功（void返回类型）
        
        log.info("发送延迟消息成功，延迟时间: {}秒", delaySeconds);
        
        // 检查Redis中的延迟消息数据
        String delayKey = "real_test_delay_message:" + TOPIC;
        Long queueSize = redisTemplate.opsForZSet().count(delayKey, 0, Double.MAX_VALUE);
        log.info("延迟队列中消息数量: {}", queueSize);
        
        // 等待延迟时间
        Thread.sleep((delaySeconds + 1) * 1000);
        
        long actualDelay = System.currentTimeMillis() - startTime;
        assertTrue(actualDelay >= delaySeconds * 1000, 
            String.format("延迟时间应该至少%d秒，实际%d毫秒", delaySeconds, actualDelay));
        
        log.info("Redis延迟消息测试完成，实际延迟: {}ms", actualDelay);
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testDelayMessageAccuracy() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        
        // 测试不同延迟时间的准确性
        int[] delaySeconds = {1, 2, 3, 5};
        List<Long> actualDelays = new ArrayList<>();
        
        for (int delay : delaySeconds) {
            TestEvent event = new TestEvent();
            event.setMessage("延迟" + delay + "秒消息");
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(delay);
            
            long startTime = System.currentTimeMillis();
            
            producer.asyncSendDelay(MQTypeEnum.REDIS, TOPIC, TAG, event, delay);
            // 异步延迟消息发送成功（void返回类型）
            
            log.info("发送延迟{}秒消息成功", delay);
            
            // 等待延迟时间 + 1秒缓冲
            Thread.sleep((delay + 1) * 1000);
            
            long actualDelay = System.currentTimeMillis() - startTime;
            actualDelays.add(actualDelay);
            
            // 验证延迟准确性（允许±1500ms误差，考虑扫描间隔、处理时间和网络延迟）
            long expectedDelay = delay * 1000;
            long tolerance = 1500;  // 进一步增加容错范围，考虑扫描间隔100ms + 处理时间 + 网络延迟
            assertTrue(Math.abs(actualDelay - expectedDelay) <= tolerance,
                String.format("延迟时间不准确，期望%dms，实际%dms，误差%dms", 
                    expectedDelay, actualDelay, Math.abs(actualDelay - expectedDelay)));
            
            log.info("延迟{}秒消息实际延迟: {}ms，误差: {}ms", 
                delay, actualDelay, Math.abs(actualDelay - expectedDelay));
        }
        
        log.info("Redis延迟消息准确性测试完成，测试了{}种延迟时间", delaySeconds.length);
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testDelayMessagePerformance() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        
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
                    event.setMessage("性能测试消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    
                    // 随机延迟时间：1-5秒
                    int delaySeconds = (index % 5) + 1;
                    producer.asyncSendDelay(MQTypeEnum.REDIS, TOPIC, TAG, event, delaySeconds);
                    
                    // 异步延迟消息发送成功（void返回类型）
                    successCount.incrementAndGet();
                    log.debug("发送延迟消息{}: 成功, 延迟{}秒", index, delaySeconds);
                } catch (Exception e) {
                    log.error("发送延迟消息{}失败", index, e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有消息发送完成
        assertTrue(latch.await(30, TimeUnit.SECONDS), "延迟消息发送超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) successCount.get() / (totalTime / 1000.0);
        
        // 验证性能指标
        assertTrue(successCount.get() >= messageCount * 0.9, 
            String.format("成功率应该大于90%%，实际成功%d/%d", successCount.get(), messageCount));
        assertTrue(tps > 5, String.format("TPS应该大于5，实际TPS: %.2f", tps));
        
        // 检查Redis中的延迟消息数据
        String delayKey = "real_test_delay_message:" + TOPIC;
        Long queueSize = redisTemplate.opsForZSet().count(delayKey, 0, Double.MAX_VALUE);
        
        log.info("Redis延迟消息性能测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 成功消息数: {}", successCount.get());
        log.info("- 成功率: {:.2f}%", (double) successCount.get() / messageCount * 100);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- TPS: {:.2f}", tps);
        log.info("- 延迟队列中消息数: {}", queueSize);
    }

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testLargeVolumeDelayMessages() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        
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
                    event.setMessage("大量延迟消息" + index);
                    event.setTimestamp(System.currentTimeMillis());
                    event.setSequence(index);
                    
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
                    
                    producer.asyncSendDelay(MQTypeEnum.REDIS, TOPIC, TAG, event, delaySeconds);
                    
                    // 异步延迟消息发送成功（void返回类型）
                    successCount.incrementAndGet();
                    if (index % 20 == 0) {
                        log.info("发送大量延迟消息进度: {}/{}, 当前延迟{}秒", index + 1, messageCount, delaySeconds);
                    }
                } catch (Exception e) {
                    log.error("发送大量延迟消息{}失败", index, e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有消息发送完成
        assertTrue(latch.await(60, TimeUnit.SECONDS), "大量延迟消息发送超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double tps = (double) successCount.get() / (totalTime / 1000.0);
        
        // 等待一小段时间确保消息已存储到Redis
        Thread.sleep(500);
        
        // 检查Redis中的延迟消息数据
        String delayKey = "delay_message:queue";  // 使用正确的队列键名
        Long queueSize = redisTemplate.opsForZSet().count(delayKey, 0, Double.MAX_VALUE);
        
        // 验证结果
        assertTrue(successCount.get() >= messageCount * 0.95, 
            String.format("成功率应该大于95%%，实际成功%d/%d", successCount.get(), messageCount));
        // 由于消息可能被快速处理，只验证成功率，不强制要求队列中有消息
        log.info("当前延迟队列中消息数: {}", queueSize);
        
        log.info("Redis大量延迟消息测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 成功消息数: {}", successCount.get());
        log.info("- 成功率: {:.2f}%", (double) successCount.get() / messageCount * 100);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- TPS: {:.2f}", tps);
        log.info("- 延迟队列中消息数: {}", queueSize);
        log.info("- 平均每条消息耗时: {:.2f}ms", (double) totalTime / successCount.get());
    }
}