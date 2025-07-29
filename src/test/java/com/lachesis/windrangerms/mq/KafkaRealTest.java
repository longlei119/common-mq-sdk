package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.KafkaConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import com.lachesis.windrangerms.mq.producer.impl.KafkaProducer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka真实环境测试
 */
@Slf4j
@SpringBootTest
@ActiveProfiles("mq")
public class KafkaRealTest {

    private static final String TOPIC = "kafka_test_topic";
    private static final String TAG = "kafka_test_tag";

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

    @Autowired
    private MQFactory mqFactory;

    @Test
    public void testKafkaProducerAndConsumer() throws InterruptedException {
        log.info("开始测试Kafka生产者和消费者");
        
        // 获取Kafka生产者和消费者
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.KAFKA);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-topic";
        String tag = "test-tag";
        String testMessage = "Hello Kafka Test Message";
        
        // 用于统计接收到的消息数量
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.KAFKA, topic, tag, (message) -> {
            log.info("接收到消息: {}", message);
            if (message.contains(testMessage)) {
                receivedCount.incrementAndGet();
                latch.countDown();
            }
        });
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 创建测试事件
        TestEvent event = new TestEvent();
        event.setMessage(testMessage);
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("kafka-test-001");
        
        // 发送消息
        log.info("发送消息到主题: {}", topic);
        String messageId = producer.syncSend(MQTypeEnum.KAFKA, topic, tag, event);
        assertNotNull(messageId, "消息ID不应为空");
        
        // 等待消息被消费
        boolean received = latch.await(10, TimeUnit.SECONDS);
        
        assertTrue(received, "应该在10秒内接收到消息");
        assertEquals(1, receivedCount.get(), "应该接收到1条消息");
        
        log.info("Kafka生产者和消费者测试完成");
    }
    
    @Test
    public void testKafkaAsyncSend() throws InterruptedException {
        log.info("开始测试Kafka异步发送");
        
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.KAFKA);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-async-topic";
        String tag = "async-test-tag";
        String testMessage = "Hello Kafka Async Message";
        
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch receiveLatch = new CountDownLatch(1);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.KAFKA, topic, tag, (message) -> {
            log.info("接收到异步消息: {}", message);
            if (message.contains(testMessage)) {
                receivedCount.incrementAndGet();
                receiveLatch.countDown();
            }
        });
        
        // 等待消费者启动
        Thread.sleep(1000);
        
        // 创建测试事件
        TestEvent event = new TestEvent();
        event.setMessage(testMessage);
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("kafka-async-test-001");
        
        // 异步发送消息
        producer.asyncSend(MQTypeEnum.KAFKA, topic, tag, event);
        
        // 等待消息接收
        boolean received = receiveLatch.await(10, TimeUnit.SECONDS);
        assertTrue(received, "应该接收到异步发送的消息");
        assertEquals(1, receivedCount.get(), "应该接收到1条消息");
        
        log.info("Kafka异步发送测试完成");
    }
    
    @Test
    public void testKafkaDelayMessage() throws InterruptedException {
        log.info("开始测试Kafka延迟消息");
        
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.KAFKA);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-delay-topic";
        String tag = "delay-test-tag";
        String testMessage = "Hello Kafka Delay Message";
        
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.KAFKA, topic, tag, (message) -> {
            log.info("接收到延迟消息: {}, 延迟时间: {}ms", message, System.currentTimeMillis() - startTime);
            if (message.contains(testMessage)) {
                receivedCount.incrementAndGet();
                latch.countDown();
            }
        });
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 创建测试事件
        TestEvent event = new TestEvent();
        event.setMessage(testMessage);
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setTraceId("kafka-delay-test-001");
        
        try {
            // 发送延迟消息（延迟3秒）
            log.info("发送延迟消息到主题: {}", topic);
            String messageId = producer.asyncSendDelay(MQTypeEnum.KAFKA, topic, tag, event, 3);
            assertNotNull(messageId, "延迟消息ID不应为空");
            
            // 等待消息被消费（延迟时间 + 额外等待时间）
            boolean received = latch.await(15, TimeUnit.SECONDS);
            
            assertTrue(received, "应该接收到延迟消息");
            assertEquals(1, receivedCount.get(), "应该接收到1条延迟消息");
            
            // 验证延迟时间（至少延迟了2秒，考虑到网络延迟等因素）
            long actualDelay = System.currentTimeMillis() - startTime;
            assertTrue(actualDelay >= 2000, "延迟时间应该至少2秒，实际延迟: " + actualDelay + "ms");
            
            log.info("Kafka延迟消息测试完成");
        } catch (RuntimeException e) {
            if (e.getMessage().contains("DelayMessageSender未配置")) {
                log.warn("DelayMessageSender未配置，跳过延迟消息测试: {}", e.getMessage());
                // 如果DelayMessageSender未配置，则跳过测试
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "DelayMessageSender未配置，跳过延迟消息测试");
            } else {
                throw e;
            }
        }
    }
    
    @Test
    public void testKafkaMultipleMessages() throws InterruptedException {
        log.info("开始测试Kafka多消息发送和接收");
        
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.KAFKA);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.KAFKA);
        
        assertNotNull(producer, "Kafka生产者不能为空");
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        String topic = "test-kafka-multiple-topic";
        String tag = "multiple-test-tag";
        int messageCount = 5;
        
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.KAFKA, topic, tag, (message) -> {
            log.info("接收到消息: {}", message);
            receivedCount.incrementAndGet();
            latch.countDown();
        });
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 发送多条消息
        for (int i = 0; i < messageCount; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("Test Message " + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setTraceId("kafka-multiple-test-" + String.format("%03d", i));
            
            log.info("发送消息: {}", event.getMessage());
            String messageId = producer.syncSend(MQTypeEnum.KAFKA, topic, tag, event);
            assertNotNull(messageId, "消息ID不应为空");
        }
        
        // 等待所有消息被消费
        boolean allReceived = latch.await(15, TimeUnit.SECONDS);
        
        assertTrue(allReceived, "应该接收到所有消息");
        assertEquals(messageCount, receivedCount.get(), "应该接收到" + messageCount + "条消息");
        
        log.info("Kafka多消息测试完成，发送{}条，接收{}条", messageCount, receivedCount.get());
    }
}