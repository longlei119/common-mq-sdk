package com.lachesis.windrangerms.mq.deadletter;

import com.lachesis.windrangerms.mq.consumer.impl.KafkaConsumer;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.impl.KafkaProducer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka死信队列测试
 */
@Slf4j
@SpringBootTest
@ActiveProfiles("mq")
public class KafkaDeadLetterTest {

    @Autowired
    private MQFactory mqFactory;
    
    @Autowired
    private AdminClient kafkaAdminClient;
    
    @Autowired(required = false)
    private DeadLetterServiceFactory deadLetterServiceFactory;
    
    @BeforeEach
    public void setUp() {
        log.info("开始创建测试所需的Kafka topic");
        createTestTopics();
    }
    
    /**
     * 创建测试所需的topic
     */
    private void createTestTopics() {
        try {
            // 定义所有测试需要的topic
            String[] topicNames = {
                "test-kafka-deadletter-topic",
                "test-kafka-retry-topic", 
                "test-kafka-recovery-topic",
                "test-kafka-recovery-topic-recovery",
                "test-kafka-error-types-topic"
            };
            
            // 检查哪些topic不存在
            java.util.Set<String> existingTopics = kafkaAdminClient.listTopics().names().get();
            java.util.List<NewTopic> topicsToCreate = new java.util.ArrayList<>();
            
            for (String topicName : topicNames) {
                if (!existingTopics.contains(topicName)) {
                    topicsToCreate.add(new NewTopic(topicName, 1, (short) 1));
                    log.info("准备创建topic: {}", topicName);
                }
            }
            
            // 创建不存在的topic
            if (!topicsToCreate.isEmpty()) {
                CreateTopicsResult result = kafkaAdminClient.createTopics(topicsToCreate);
                result.all().get(30, TimeUnit.SECONDS); // 等待创建完成，最多30秒
                log.info("成功创建 {} 个topic", topicsToCreate.size());
            } else {
                log.info("所有测试topic都已存在，无需创建");
            }
            
        } catch (Exception e) {
            log.warn("创建测试topic失败，测试可能会失败: {}", e.getMessage());
        }
    }

    @Test
    public void testKafkaMessageFailureToDeadLetter() throws InterruptedException {
        log.info("开始测试Kafka消息处理失败进入死信队列");
        
        // 获取Kafka生产者和消费者
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-deadletter-topic";
        String failureMessage = "This message will fail processing";
        
        AtomicInteger processAttempts = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息 - 模拟处理失败
        consumer.subscribeUnicast(topic, null, (message) -> {
            int attempts = processAttempts.incrementAndGet();
            log.info("处理消息尝试 #{}: {}", attempts, message);
            
            // 从JSON消息中提取实际内容进行比较
            if (message.contains(failureMessage)) {
                // 模拟处理失败
                log.error("模拟消息处理失败: {}", message);
                if (attempts >= 3) {
                    // 达到最大重试次数后，让测试继续
                    latch.countDown();
                }
                throw new RuntimeException("模拟处理失败");
            }
        }, "test-group");
        
        // 启动消费者
        consumer.start();
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 发送会失败的消息
        log.info("发送会处理失败的消息: {}", failureMessage);
        TestEvent failureEvent = new TestEvent(failureMessage, topic, null);
        producer.syncSend(MQTypeEnum.KAFKA, topic, null, failureEvent);
        
        // 等待消息处理失败并进入死信队列
        boolean processed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(processed, "消息应该被处理（即使失败）");
        
        // 验证消息处理尝试次数
        assertTrue(processAttempts.get() >= 3, "应该至少尝试处理3次，实际尝试次数: " + processAttempts.get());
        
        // 等待一段时间让死信队列处理完成
        Thread.sleep(3000);
        
        // 验证死信队列中是否有消息（如果死信队列服务可用）
        if (deadLetterServiceFactory != null) {
            DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
            if (deadLetterService != null) {
                // 检查死信队列中的消息
                // 注意：这里的验证方式取决于具体的死信队列实现
                log.info("验证死信队列中的消息");
            } else {
                log.warn("死信队列服务未配置，跳过死信队列验证");
            }
        } else {
            log.warn("死信队列服务工厂未配置，跳过死信队列验证");
        }
        
        // 停止消费
        consumer.stop();
        
        log.info("Kafka消息失败进入死信队列测试完成");
    }
    
    @Test
    public void testKafkaMessageRetryMechanism() throws InterruptedException {
        log.info("开始测试Kafka消息重试机制");
        
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-retry-topic";
        String retryMessage = "This message will succeed after retries";
        
        AtomicInteger processAttempts = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息 - 前几次失败，最后一次成功
        consumer.subscribeUnicast(topic, null, (message) -> {
            int attempts = processAttempts.incrementAndGet();
            log.info("处理消息尝试 #{}: {}", attempts, message);
            
            // 从JSON消息中提取实际内容进行比较
            if (message.contains(retryMessage)) {
                if (attempts < 3) {
                    // 前两次处理失败
                    log.warn("模拟消息处理失败，尝试次数: {}", attempts);
                    throw new RuntimeException("模拟处理失败");
                } else {
                    // 第三次处理成功
                    log.info("消息处理成功，尝试次数: {}", attempts);
                    latch.countDown();
                }
            }
        }, "test-retry-group");
        
        // 启动消费者
        consumer.start();
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 发送需要重试的消息
        log.info("发送需要重试的消息: {}", retryMessage);
        TestEvent retryEvent = new TestEvent(retryMessage, topic, null);
        producer.syncSend(MQTypeEnum.KAFKA, topic, null, retryEvent);
        
        // 等待消息最终处理成功
        boolean processed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(processed, "消息应该在重试后处理成功");
        
        // 验证重试次数
        assertEquals(3, processAttempts.get(), "应该尝试处理3次");
        
        // 停止消费
        consumer.stop();
        
        log.info("Kafka消息重试机制测试完成");
    }
    
    @Test
    public void testKafkaDeadLetterQueueRecovery() throws InterruptedException {
        log.info("开始测试Kafka死信队列消息恢复");
        
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-recovery-topic";
        String recoveryMessage = "This message will be recovered from dead letter";
        
        AtomicInteger failureAttempts = new AtomicInteger(0);
        AtomicInteger successAttempts = new AtomicInteger(0);
        CountDownLatch failureLatch = new CountDownLatch(1);
        CountDownLatch successLatch = new CountDownLatch(1);
        
        // 第一阶段：模拟消息处理失败
        consumer.subscribeUnicast(topic, null, (message) -> {
            int attempts = failureAttempts.incrementAndGet();
            log.info("失败阶段 - 处理消息尝试 #{}: {}", attempts, message);
            
            // 从JSON消息中提取实际内容进行比较
            if (message.contains(recoveryMessage)) {
                log.error("模拟消息处理失败: {}", message);
                if (attempts >= 3) {
                    failureLatch.countDown();
                }
                throw new RuntimeException("模拟处理失败");
            }
        }, "test-recovery-group");
        
        // 启动消费者
        consumer.start();
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 发送消息
        log.info("发送消息（将失败并进入死信队列）: {}", recoveryMessage);
        TestEvent recoveryEvent = new TestEvent(recoveryMessage, topic, null);
        producer.syncSend(MQTypeEnum.KAFKA, topic, null, recoveryEvent);
        
        // 等待消息处理失败
        boolean failed = failureLatch.await(30, TimeUnit.SECONDS);
        assertTrue(failed, "消息应该处理失败");
        
        // 停止当前消费者
        consumer.stop();
        
        // 等待死信队列处理
        Thread.sleep(3000);
        
        // 第二阶段：模拟从死信队列恢复消息并成功处理
        log.info("开始从死信队列恢复消息");
        
        // 创建新的消费者用于恢复
        KafkaConsumer recoveryConsumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        // 设置新的消息处理器 - 这次会成功
        recoveryConsumer.subscribeUnicast(topic + "-recovery", null, (message) -> {
            int attempts = successAttempts.incrementAndGet();
            log.info("恢复阶段 - 处理消息尝试 #{}: {}", attempts, message);
            
            // 从JSON消息中提取实际内容进行比较
            if (message.contains(recoveryMessage)) {
                log.info("从死信队列恢复的消息处理成功: {}", message);
                successLatch.countDown();
            }
        }, "test-recovery-success-group");
        
        // 这里应该从死信队列中恢复消息，具体实现取决于死信队列的设计
        // 为了测试，我们重新发送消息模拟恢复过程
        recoveryConsumer.start();
        Thread.sleep(2000);
        
        // 模拟从死信队列恢复消息
        log.info("模拟从死信队列恢复消息: {}", recoveryMessage);
        TestEvent recoveryEvent2 = new TestEvent(recoveryMessage, topic + "-recovery", null);
        producer.syncSend(MQTypeEnum.KAFKA, topic + "-recovery", null, recoveryEvent2);
        
        // 等待恢复的消息处理成功
        boolean recovered = successLatch.await(15, TimeUnit.SECONDS);
        assertTrue(recovered, "恢复的消息应该处理成功");
        
        // 验证处理次数
        assertTrue(failureAttempts.get() >= 3, "失败阶段应该至少尝试3次");
        assertEquals(1, successAttempts.get(), "恢复阶段应该成功处理1次");
        
        // 停止恢复消费者
        recoveryConsumer.stop();
        
        log.info("Kafka死信队列消息恢复测试完成");
    }
    
    @Test
    public void testKafkaDeadLetterWithDifferentErrorTypes() throws InterruptedException {
        log.info("开始测试Kafka不同错误类型的死信队列处理");
        
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-error-types-topic";
        
        AtomicInteger runtimeErrorCount = new AtomicInteger(0);
        AtomicInteger businessErrorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2); // 期望处理2种类型的错误
        
        // 订阅消息 - 模拟不同类型的错误
        consumer.subscribeUnicast(topic, null, (message) -> {
            log.info("处理消息: {}", message);
            
            // 从JSON消息中提取实际内容进行比较
            if (message.contains("runtime-error")) {
                int attempts = runtimeErrorCount.incrementAndGet();
                log.error("模拟运行时异常，尝试次数: {}", attempts);
                if (attempts >= 3) {
                    latch.countDown();
                }
                throw new RuntimeException("模拟运行时异常");
            } else if (message.contains("business-error")) {
                int attempts = businessErrorCount.incrementAndGet();
                log.error("模拟业务逻辑错误，尝试次数: {}", attempts);
                if (attempts >= 3) {
                    latch.countDown();
                }
                throw new IllegalArgumentException("模拟业务逻辑错误");
            }
        }, "test-error-types-group");
        
        // 启动消费者
        consumer.start();
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 发送会引起运行时异常的消息
        log.info("发送会引起运行时异常的消息");
        TestEvent runtimeErrorEvent = new TestEvent("runtime-error message", topic, null);
        producer.syncSend(MQTypeEnum.KAFKA, topic, null, runtimeErrorEvent);
        
        // 发送会引起业务逻辑错误的消息
        log.info("发送会引起业务逻辑错误的消息");
        TestEvent businessErrorEvent = new TestEvent("business-error message", topic, null);
        producer.syncSend(MQTypeEnum.KAFKA, topic, null, businessErrorEvent);
        
        // 等待所有错误类型都被处理
        boolean allProcessed = latch.await(45, TimeUnit.SECONDS);
        assertTrue(allProcessed, "所有错误类型都应该被处理");
        
        // 验证错误处理次数
        assertTrue(runtimeErrorCount.get() >= 3, "运行时错误应该至少处理3次");
        assertTrue(businessErrorCount.get() >= 3, "业务逻辑错误应该至少处理3次");
        
        // 停止消费
        consumer.stop();
        
        log.info("Kafka不同错误类型的死信队列处理测试完成");
    }
    
    /**
     * 测试事件类
     */
    @Data
    static class TestEvent extends MQEvent {
        private String message;
        private String actualTopic;
        private String actualTag;
        
        public TestEvent(String message, String topic, String tag) {
            this.message = message;
            this.actualTopic = topic;
            this.actualTag = tag;
        }
        
        @Override
        public String getTopic() {
            return actualTopic;
        }
        
        @Override
        public String getTag() {
            return actualTag;
        }
    }
}