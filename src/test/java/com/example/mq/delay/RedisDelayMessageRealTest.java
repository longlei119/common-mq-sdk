package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.consumer.impl.RedisConsumer;
import com.example.mq.delay.adapter.RedisAdapter;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redis延迟消息真实测试
 * 测试真实的延迟消息发送和接收效果
 */
@Slf4j
public class RedisDelayMessageRealTest {

    private StringRedisTemplate redisTemplate;
    private DelayMessageSender delayMessageSender;
    private RedisConsumer redisConsumer;
    private RedisMessageListenerContainer listenerContainer;
    private MQConfig mqConfig;

    @BeforeEach
    public void setup() {
        try {
            // 创建Redis连接工厂（使用本地Redis）
            LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory("localhost", 6379);
            connectionFactory.afterPropertiesSet();

            // 创建Redis模板
            redisTemplate = new StringRedisTemplate();
            redisTemplate.setConnectionFactory(connectionFactory);
            redisTemplate.setKeySerializer(new StringRedisSerializer());
            redisTemplate.setValueSerializer(new StringRedisSerializer());
            redisTemplate.afterPropertiesSet();
            
            // 测试Redis连接
            try {
                redisTemplate.opsForValue().set("test_connection", "ok");
                String testValue = redisTemplate.opsForValue().get("test_connection");
                redisTemplate.delete("test_connection");
                log.info("Redis连接测试成功: {}", testValue);
            } catch (Exception e) {
                log.error("Redis连接失败，请确保Redis服务已启动: {}", e.getMessage());
                throw new RuntimeException("Redis连接失败", e);
            }

            // 创建Redis消息监听容器
            listenerContainer = new RedisMessageListenerContainer();
            listenerContainer.setConnectionFactory(connectionFactory);
            listenerContainer.afterPropertiesSet();
            listenerContainer.start();

            // 创建Redis消费者
            redisConsumer = new RedisConsumer(listenerContainer);

            // 配置延迟消息
            mqConfig = new MQConfig();
            MQConfig.DelayMessageProperties delayProps = new MQConfig.DelayMessageProperties();
            delayProps.setRedisKeyPrefix("real_test_delay_message:");
            delayProps.setScanInterval(1000); // 1秒扫描一次
            delayProps.setBatchSize(10);
            delayProps.setMessageExpireTime(7 * 24 * 60 * 60 * 1000L);
            
            // 重试配置
            MQConfig.DelayMessageProperties.RetryConfig retryConfig = new MQConfig.DelayMessageProperties.RetryConfig();
            retryConfig.setMaxRetries(3);
            retryConfig.setRetryInterval(1000);
            retryConfig.setRetryMultiplier(2);
            delayProps.setRetry(retryConfig);
            
            mqConfig.setDelay(delayProps);

            // 创建Redis适配器
            RedisAdapter redisAdapter = new RedisAdapter(redisTemplate);
            List<com.example.mq.delay.adapter.MQAdapter> adapters = new ArrayList<>();
            adapters.add(redisAdapter);

            // 创建延迟消息发送器
            delayMessageSender = new DelayMessageSender(redisTemplate, adapters, mqConfig);
            
            log.info("Redis延迟消息测试环境初始化完成");
        } catch (Exception e) {
            log.error("初始化测试环境失败: {}", e.getMessage(), e);
            throw new RuntimeException("初始化测试环境失败", e);
        }
    }

    @Test
    public void testRealRedisDelayMessage() throws Exception {
        log.info("=== 开始Redis延迟消息真实测试 ===");
        
        // 测试参数
        String topic = "test-delay-topic";
        String tag = "test-delay-tag";
        String messageBody = "这是一条延迟10秒的测试消息 - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        long delaySeconds = 10; // 延迟10秒
        
        // 用于接收消息的变量
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        long sendTime = System.currentTimeMillis();
        
        log.info("发送时间: {}", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        log.info("预期接收时间: {}", LocalDateTime.now().plusSeconds(delaySeconds).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        
        // 订阅消息
        String channel = topic + ":" + tag;
        redisConsumer.subscribe(MQTypeEnum.REDIS, topic, tag, message -> {
            long receiveTime = System.currentTimeMillis();
            long actualDelay = receiveTime - sendTime;
            
            log.info("接收时间: {}", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
            log.info("实际延迟: {}毫秒 ({}秒)", actualDelay, actualDelay / 1000.0);
            log.info("接收到的消息: {}", message);
            
            receivedMessage.set(message);
            latch.countDown();
        });
        
        log.info("已订阅Redis通道: {}", channel);
        
        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(
            topic, 
            tag, 
            messageBody, 
            MQTypeEnum.REDIS.getType(), 
            delaySeconds * 1000 // 转换为毫秒
        );
        
        log.info("延迟消息已发送，消息ID: {}", messageId);
        log.info("消息内容: {}", messageBody);
        log.info("延迟时间: {}秒", delaySeconds);
        
        // 验证消息是否已保存到Redis
        String messageKey = mqConfig.getDelay().getRedisKeyPrefix() + messageId;
        String queueKey = mqConfig.getDelay().getRedisKeyPrefix() + "queue";
        String savedMessage = redisTemplate.opsForValue().get(messageKey);
        Double score = redisTemplate.opsForZSet().score(queueKey, messageId);
        log.info("Redis中保存的消息: {}", savedMessage);
        log.info("消息在队列中的分数(投递时间戳): {}", score);
        
        // 启动一个线程定期扫描延迟消息
        Thread scanThread = new Thread(() -> {
            int scanCount = 0;
            while (!Thread.currentThread().isInterrupted() && scanCount < 20) { // 最多扫描20次
                try {
                    scanCount++;
                    log.info("第{}次扫描延迟消息...", scanCount);
                    delayMessageSender.scanAndDeliverMessages();
                    Thread.sleep(1000); // 每秒扫描一次
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("扫描延迟消息时发生异常: {}", e.getMessage(), e);
                }
            }
            log.info("扫描线程结束，共扫描{}次", scanCount);
        });
        scanThread.setDaemon(true);
        scanThread.start();
        
        // 等待消息接收（最多等待15秒）
        boolean received = latch.await(15, TimeUnit.SECONDS);
        
        // 停止扫描线程
        scanThread.interrupt();
        
        // 验证结果
        assertTrue(received, "应该在15秒内接收到延迟消息");
        assertNotNull(receivedMessage.get(), "接收到的消息不应为空");
        assertEquals(messageBody, receivedMessage.get(), "接收到的消息内容应该与发送的一致");
        
        log.info("=== Redis延迟消息真实测试完成 ===");
        
        // 清理资源
        try {
            redisConsumer.unsubscribe(MQTypeEnum.REDIS, topic, tag);
            if (listenerContainer != null && listenerContainer.isRunning()) {
                listenerContainer.stop();
            }
            // 清理Redis中的测试数据
            redisTemplate.delete(messageKey);
            redisTemplate.opsForZSet().remove(queueKey, messageId);
        } catch (Exception e) {
            log.warn("清理资源时发生异常: {}", e.getMessage());
        }
    }
    
    @Test
    public void testMultipleDelayMessages() throws Exception {
        log.info("=== 开始多条延迟消息测试 ===");
        
        String topic = "multi-delay-topic";
        String tag = "multi-delay-tag";
        
        // 用于接收消息的变量
        List<String> receivedMessages = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(3); // 期望接收3条消息
        long startTime = System.currentTimeMillis();
        
        // 订阅消息
        redisConsumer.subscribe(MQTypeEnum.REDIS, topic, tag, message -> {
            long receiveTime = System.currentTimeMillis();
            long elapsed = receiveTime - startTime;
            
            log.info("接收到消息: {} (耗时: {}ms)", message, elapsed);
            receivedMessages.add(message);
            latch.countDown();
        });
        
        // 发送3条不同延迟时间的消息
        String msg1 = "消息1 - 延迟3秒";
        String msg2 = "消息2 - 延迟6秒";
        String msg3 = "消息3 - 延迟9秒";
        
        delayMessageSender.sendDelayMessage(topic, tag, msg1, MQTypeEnum.REDIS.getType(), 3000);
        delayMessageSender.sendDelayMessage(topic, tag, msg2, MQTypeEnum.REDIS.getType(), 6000);
        delayMessageSender.sendDelayMessage(topic, tag, msg3, MQTypeEnum.REDIS.getType(), 9000);
        
        log.info("已发送3条延迟消息");
        
        // 启动扫描线程
        Thread scanThread = new Thread(() -> {
            try {
                for (int i = 0; i < 12; i++) { // 扫描12次，每次间隔1秒
                    Thread.sleep(1000);
                    delayMessageSender.scanAndDeliverMessages();
                }
            } catch (Exception e) {
                log.error("扫描异常: {}", e.getMessage(), e);
            }
        });
        scanThread.start();
        
        // 等待所有消息接收
        boolean allReceived = latch.await(15, TimeUnit.SECONDS);
        
        // 验证结果
        assertTrue(allReceived, "应该接收到所有3条延迟消息");
        assertEquals(3, receivedMessages.size(), "应该接收到3条消息");
        
        log.info("接收到的消息顺序: {}", receivedMessages);
        log.info("=== 多条延迟消息测试完成 ===");
        
        // 清理资源
        redisConsumer.unsubscribe(MQTypeEnum.REDIS, topic, tag);
    }
}