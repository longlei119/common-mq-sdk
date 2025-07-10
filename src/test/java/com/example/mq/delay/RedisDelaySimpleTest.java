package com.example.mq.delay;

import com.example.mq.delay.DelayMessageSender;
import com.example.mq.config.MQConfig;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.delay.adapter.MQAdapter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.ArrayList;
import java.util.List;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Redis延迟消息简单测试 - 直接观察控制台输出
 */
@Slf4j
public class RedisDelaySimpleTest {

    private DelayMessageSender delayMessageSender;
    private StringRedisTemplate redisTemplate;
    private Thread scanThread;
    private volatile boolean shouldStop = false;

    @BeforeEach
    public void setup() {
        // 创建Redis连接
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory("localhost", 6379);
        connectionFactory.setDatabase(0);
        connectionFactory.afterPropertiesSet();

        // 创建StringRedisTemplate
        redisTemplate = new StringRedisTemplate();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.afterPropertiesSet();

        // 创建MQConfig
        MQConfig mqConfig = new MQConfig();
        MQConfig.DelayMessageProperties delayProps = new MQConfig.DelayMessageProperties();
        delayProps.setRedisKeyPrefix("delay_message:");
        mqConfig.setDelay(delayProps);

        // 创建模拟的MQAdapter
        List<MQAdapter> mqAdapters = new ArrayList<>();
        mqAdapters.add(new MQAdapter() {
            @Override
            public boolean send(DelayMessage message) {
                log.info("✅ 模拟发送延迟消息: topic={}, tag={}, body={}, mqType={}", 
                        message.getTopic(), message.getTag(), 
                        new String(message.getBody()), message.getMqTypeEnum());
                return true; // 模拟发送成功
            }
            
            @Override
            public String getMQType() {
                return "REDIS";
            }
        });
        
        // 创建DelayMessageSender
        delayMessageSender = new DelayMessageSender(redisTemplate, mqAdapters, mqConfig);

        log.info("测试环境初始化完成");
    }

    @Test
    public void testSimpleDelayMessage() throws InterruptedException {
        log.info("=== 开始Redis延迟消息简单测试 ===");
        
        String testMessage = "测试延迟消息-" + System.currentTimeMillis();
        String topic = "test-delay-topic";
        int delaySeconds = 5;
        
        // 记录发送时间
        LocalDateTime sendTime = LocalDateTime.now();
        log.info("发送时间: {}", sendTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        log.info("发送消息: {}, 延迟: {}秒", testMessage, delaySeconds);
        
        // 发送延迟消息
        log.info("=== 开始发送延迟消息 ===");
        log.info("消息内容: {}", testMessage);
        log.info("延迟消息发送时间: {}", sendTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        log.info("预期执行时间: {}", sendTime.plusSeconds(delaySeconds).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        
        delayMessageSender.sendDelayMessage(topic, "test-tag", testMessage, "REDIS", delaySeconds * 1000L);
        log.info("延迟消息已发送到Redis");
        
        // 检查Redis中是否保存了消息
        String messageKey = "delay_message:" + topic;
        String queueKey = "delay_queue:" + topic;
        
        log.info("检查Redis中的消息存储:");
        log.info("消息Key: {}, 存在: {}", messageKey, redisTemplate.hasKey(messageKey));
        log.info("队列Key: {}, 存在: {}", queueKey, redisTemplate.hasKey(queueKey));
        
        if (redisTemplate.hasKey(queueKey)) {
            Long queueSize = redisTemplate.opsForZSet().size(queueKey);
            log.info("延迟队列大小: {}", queueSize);
        }
        
        // 启动扫描线程，模拟定时任务
        CountDownLatch latch = new CountDownLatch(1);
        shouldStop = false;
        
        scanThread = new Thread(() -> {
            int scanCount = 0;
            while (!shouldStop && scanCount < 20) { // 最多扫描20次，避免无限循环
                try {
                    log.info("第{}次扫描延迟消息...", scanCount + 1);
                    
                    // 检查是否有到期的消息
                    Long currentTime = System.currentTimeMillis();
                    log.info("当前时间戳: {}", currentTime);
                    
                    // 调用扫描方法
                    delayMessageSender.scanAndDeliverMessages();
                    
                    // 检查消息是否已被处理
                    if (redisTemplate.hasKey(queueKey)) {
                        Long remainingSize = redisTemplate.opsForZSet().size(queueKey);
                        log.info("扫描后队列剩余大小: {}", remainingSize);
                        if (remainingSize == 0) {
                            log.info("✅ 延迟消息已被处理！");
                            LocalDateTime receiveTime = LocalDateTime.now();
                            log.info("处理时间: {}", receiveTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
                            
                            long actualDelay = java.time.Duration.between(sendTime, receiveTime).getSeconds();
                            log.info("实际延迟: {}秒 (预期: {}秒)", actualDelay, delaySeconds);
                            
                            latch.countDown();
                            break;
                        }
                    } else {
                        log.info("延迟队列已不存在，消息可能已被处理");
                        latch.countDown();
                        break;
                    }
                    
                    scanCount++;
                    Thread.sleep(500); // 每0.5秒扫描一次
                } catch (Exception e) {
                    log.error("扫描过程中出现异常", e);
                }
            }
            
            if (scanCount >= 20) {
                log.warn("⚠️ 扫描次数达到上限，测试结束");
                latch.countDown();
            }
        });
        
        scanThread.start();
        
        // 等待测试完成，最多等待15秒
        boolean completed = latch.await(15, TimeUnit.SECONDS);
        shouldStop = true;
        
        if (completed) {
            log.info("✅ 测试完成");
        } else {
            log.warn("⚠️ 测试超时，未在预期时间内完成");
        }
        
        // 清理测试数据
        cleanup();
        
        log.info("=== Redis延迟消息简单测试结束 ===");
    }
    
    private void cleanup() {
        try {
            if (scanThread != null && scanThread.isAlive()) {
                shouldStop = true;
                scanThread.interrupt();
            }
            
            // 清理Redis中的测试数据
            String messageKey = "delay_message:test-delay-topic";
            String queueKey = "delay_queue:test-delay-topic";
            
            if (redisTemplate.hasKey(messageKey)) {
                redisTemplate.delete(messageKey);
                log.info("清理消息Key: {}", messageKey);
            }
            
            if (redisTemplate.hasKey(queueKey)) {
                redisTemplate.delete(queueKey);
                log.info("清理队列Key: {}", queueKey);
            }
            
            log.info("测试数据清理完成");
        } catch (Exception e) {
            log.error("清理过程中出现异常", e);
        }
    }
}