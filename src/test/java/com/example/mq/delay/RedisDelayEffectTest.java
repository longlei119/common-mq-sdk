package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.consumer.impl.RedisConsumer;
import com.example.mq.delay.adapter.RedisAdapter;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis延迟消息效果测试
 * 专门用于验证延迟消息的时间效果
 */
@Slf4j
public class RedisDelayEffectTest {

    private StringRedisTemplate redisTemplate;
    private RedisMessageListenerContainer listenerContainer;
    private RedisConsumer redisConsumer;
    private DelayMessageSender delayMessageSender;
    private MQConfig mqConfig;

    @BeforeEach
    public void setup() {
        try {
            // 创建Redis连接
            LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory("localhost", 6379);
            connectionFactory.afterPropertiesSet();

            // 创建Redis模板
            redisTemplate = new StringRedisTemplate();
            redisTemplate.setConnectionFactory(connectionFactory);
            redisTemplate.setKeySerializer(new StringRedisSerializer());
            redisTemplate.setValueSerializer(new StringRedisSerializer());
            redisTemplate.afterPropertiesSet();

            // 测试Redis连接
            redisTemplate.opsForValue().set("test_ping", "pong");
            redisTemplate.delete("test_ping");
            log.info("Redis连接正常");

            // 创建消息监听容器
            listenerContainer = new RedisMessageListenerContainer();
            listenerContainer.setConnectionFactory(connectionFactory);
            listenerContainer.afterPropertiesSet();
            listenerContainer.start();

            // 创建Redis消费者
            redisConsumer = new RedisConsumer(listenerContainer);

            // 配置延迟消息
            mqConfig = new MQConfig();
            MQConfig.DelayMessageProperties delayProps = new MQConfig.DelayMessageProperties();
            delayProps.setRedisKeyPrefix("delay_effect_test:");
            delayProps.setScanInterval(500); // 0.5秒扫描一次，提高响应速度
            delayProps.setBatchSize(10);
            delayProps.setMessageExpireTime(60 * 60 * 1000L); // 1小时过期
            mqConfig.setDelay(delayProps);

            // 创建Redis适配器和延迟消息发送器
            RedisAdapter redisAdapter = new RedisAdapter(redisTemplate);
            List<com.example.mq.delay.adapter.MQAdapter> adapters = new ArrayList<>();
            adapters.add(redisAdapter);
            delayMessageSender = new DelayMessageSender(redisTemplate, adapters, mqConfig);

            log.info("Redis延迟消息测试环境初始化完成");
        } catch (Exception e) {
            log.error("初始化失败: {}", e.getMessage(), e);
            throw new RuntimeException("初始化失败", e);
        }
    }

    @Test
    public void testDelayEffect() throws Exception {
        log.info("=== 开始Redis延迟消息效果测试 ===");
        
        String topic = "delay-effect-topic";
        String tag = "delay-effect-tag";
        int delaySeconds = 5; // 延迟5秒，便于快速测试
        String messageBody = "延迟消息效果测试 - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        
        // 记录发送时间
        AtomicLong sendTime = new AtomicLong();
        AtomicLong receiveTime = new AtomicLong();
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息
        redisConsumer.subscribe(MQTypeEnum.REDIS, topic, tag, (message) -> {
            receiveTime.set(System.currentTimeMillis());
            receivedMessage.set(message);
            long actualDelay = receiveTime.get() - sendTime.get();
            log.info("\n=== 消息接收成功 ===");
            log.info("接收时间: {}", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
            log.info("消息内容: {}", message);
            log.info("实际延迟: {}毫秒 (约{}秒)", actualDelay, actualDelay / 1000.0);
            log.info("预期延迟: {}秒", delaySeconds);
            log.info("延迟误差: {}毫秒", Math.abs(actualDelay - delaySeconds * 1000));
            latch.countDown();
        });
        
        // 记录发送时间并发送延迟消息
        sendTime.set(System.currentTimeMillis());
        log.info("\n=== 发送延迟消息 ===");
        log.info("发送时间: {}", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        log.info("延迟时间: {}秒", delaySeconds);
        log.info("预期接收时间: {}", LocalDateTime.now().plusSeconds(delaySeconds).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        
        String messageId = delayMessageSender.sendDelayMessage(topic, tag, messageBody, "REDIS", delaySeconds * 1000);
        log.info("消息ID: {}", messageId);
        
        // 启动扫描线程
        Thread scanThread = new Thread(() -> {
            int scanCount = 0;
            while (!Thread.currentThread().isInterrupted() && scanCount < 20) {
                try {
                    scanCount++;
                    delayMessageSender.scanAndDeliverMessages();
                    Thread.sleep(500); // 每0.5秒扫描一次
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("扫描异常: {}", e.getMessage());
                }
            }
        });
        scanThread.setDaemon(true);
        scanThread.start();
        
        // 等待消息接收（最多等待10秒）
        boolean received = latch.await(10, TimeUnit.SECONDS);
        scanThread.interrupt();
        
        // 验证结果
        assertTrue(received, "应该在10秒内接收到延迟消息");
        assertNotNull(receivedMessage.get(), "应该接收到消息内容");
        
        // 验证延迟时间（允许1秒误差）
        long actualDelay = receiveTime.get() - sendTime.get();
        long expectedDelay = delaySeconds * 1000;
        long tolerance = 1000; // 1秒误差容忍度
        
        assertTrue(Math.abs(actualDelay - expectedDelay) <= tolerance, 
                String.format("延迟时间不准确：实际%dms，预期%dms，误差%dms", 
                        actualDelay, expectedDelay, Math.abs(actualDelay - expectedDelay)));
        
        log.info("\n=== 延迟消息效果测试通过 ===");
        log.info("✓ 消息成功发送和接收");
        log.info("✓ 延迟时间符合预期（误差在{}ms内）", tolerance);
        
        // 清理资源
        try {
            redisConsumer.unsubscribe(MQTypeEnum.REDIS, topic, tag);
            if (listenerContainer != null && listenerContainer.isRunning()) {
                listenerContainer.stop();
            }
            // 清理Redis测试数据
            String messageKey = mqConfig.getDelay().getRedisKeyPrefix() + messageId;
            String queueKey = mqConfig.getDelay().getRedisKeyPrefix() + "queue";
            redisTemplate.delete(messageKey);
            redisTemplate.opsForZSet().remove(queueKey, messageId);
        } catch (Exception e) {
            log.warn("清理资源异常: {}", e.getMessage());
        }
    }
    
    @Test
    public void testMultipleDelayMessages() throws Exception {
        log.info("=== 开始多个延迟消息测试 ===");
        
        String topic = "multi-delay-topic";
        String tag = "multi-delay-tag";
        int[] delaySeconds = {2, 4, 6}; // 分别延迟2秒、4秒、6秒
        
        List<Long> sendTimes = new ArrayList<>();
        List<Long> receiveTimes = new ArrayList<>();
        List<String> receivedMessages = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(delaySeconds.length);
        
        // 订阅消息
        redisConsumer.subscribe(MQTypeEnum.REDIS, topic, tag, (message) -> {
            synchronized (receiveTimes) {
                receiveTimes.add(System.currentTimeMillis());
                receivedMessages.add(message);
                log.info("接收到消息 #{}: {}", receiveTimes.size(), message);
                latch.countDown();
            }
        });
        
        // 发送多个延迟消息
        List<String> messageIds = new ArrayList<>();
        for (int i = 0; i < delaySeconds.length; i++) {
            long sendTime = System.currentTimeMillis();
            sendTimes.add(sendTime);
            
            String messageBody = String.format("延迟消息#%d - 延迟%d秒 - %s", 
                    i + 1, delaySeconds[i], 
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
            
            String messageId = delayMessageSender.sendDelayMessage(topic, tag, messageBody, "REDIS", delaySeconds[i] * 1000);
            messageIds.add(messageId);
            
            log.info("发送消息#{}: 延迟{}秒, ID: {}", i + 1, delaySeconds[i], messageId);
        }
        
        // 启动扫描线程
        Thread scanThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    delayMessageSender.scanAndDeliverMessages();
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("扫描异常: {}", e.getMessage());
                }
            }
        });
        scanThread.setDaemon(true);
        scanThread.start();
        
        // 等待所有消息接收（最多等待10秒）
        boolean allReceived = latch.await(10, TimeUnit.SECONDS);
        scanThread.interrupt();
        
        // 验证结果
        assertTrue(allReceived, "应该接收到所有延迟消息");
        assertEquals(delaySeconds.length, receivedMessages.size(), "接收到的消息数量应该正确");
        
        log.info("\n=== 多个延迟消息测试结果 ===");
        for (int i = 0; i < receiveTimes.size(); i++) {
            long actualDelay = receiveTimes.get(i) - sendTimes.get(i);
            log.info("消息#{}: 实际延迟{}ms (约{}秒)", i + 1, actualDelay, actualDelay / 1000.0);
        }
        
        // 清理资源
        try {
            redisConsumer.unsubscribe(MQTypeEnum.REDIS, topic, tag);
            if (listenerContainer != null && listenerContainer.isRunning()) {
                listenerContainer.stop();
            }
            // 清理Redis测试数据
            for (String messageId : messageIds) {
                String messageKey = mqConfig.getDelay().getRedisKeyPrefix() + messageId;
                String queueKey = mqConfig.getDelay().getRedisKeyPrefix() + "queue";
                redisTemplate.delete(messageKey);
                redisTemplate.opsForZSet().remove(queueKey, messageId);
            }
        } catch (Exception e) {
            log.warn("清理资源异常: {}", e.getMessage());
        }
        
        log.info("=== 多个延迟消息测试通过 ===");
    }
}