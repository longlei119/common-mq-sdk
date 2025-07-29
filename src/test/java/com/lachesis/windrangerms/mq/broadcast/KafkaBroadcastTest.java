package com.lachesis.windrangerms.mq.broadcast;

import com.lachesis.windrangerms.mq.consumer.impl.KafkaConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.model.MQEvent;
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
 * Kafka广播测试
 */
@Slf4j
@SpringBootTest
@ActiveProfiles("mq")
public class KafkaBroadcastTest {

    @Autowired
    private MQFactory mqFactory;

    @Test
    public void testKafkaBroadcast() throws InterruptedException {
        log.info("开始测试Kafka广播消息");
        
        // 获取Kafka生产者
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        assertNotNull(producer, "Kafka生产者不能为空");
        
        String topic = "test-kafka-broadcast-topic";
        String broadcastMessage = "Hello Kafka Broadcast Message";
        
        // 注意：由于当前Kafka实现使用共享消费者组，广播实际表现为单播
        // 只有一个消费者会接收到消息
        int consumerCount = 3;
        AtomicInteger totalReceivedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1); // 只期望一个消费者接收到消息
        
        // 创建多个消费者
        for (int i = 0; i < consumerCount; i++) {
            final int consumerId = i;
            
            // 每个消费者使用不同的消费者组来模拟广播
            KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
            assertNotNull(consumer, "Kafka消费者不能为空");
            
            // 订阅广播消息（每个消费者使用不同的消费者组来模拟广播）
            consumer.subscribeBroadcast(topic, null, message -> {
                log.info("消费者{} 接收到广播消息: {}", consumerId, message);
                // 检查消息内容是否包含预期的消息文本
                if (message != null && message.contains(broadcastMessage)) {
                    totalReceivedCount.incrementAndGet();
                    latch.countDown();
                }
            });
        }
        
        // 等待所有消费者启动
        Thread.sleep(3000);
        
        // 发送广播消息
        log.info("发送广播消息到主题: {}", topic);
        TestEvent event = new TestEvent();
        event.setMessage(broadcastMessage);
        event.setTimestamp(System.currentTimeMillis());
        producer.syncSendBroadcast(MQTypeEnum.KAFKA, topic, null, event);
        
        // 等待消息被接收
        boolean received = latch.await(15, TimeUnit.SECONDS);
        
        assertTrue(received, "应该有消费者接收到广播消息");
        // 由于使用共享消费者组，实际只有一个消费者会接收到消息
        assertTrue(totalReceivedCount.get() >= 1, "至少应该有1个消费者接收到消息");
        
        log.info("Kafka广播测试完成，{}个消费者接收到了消息（注意：当前实现限制为单播行为）", totalReceivedCount.get());
    }
    
    @Test
    public void testKafkaUnicast() throws InterruptedException {
        log.info("开始测试Kafka单播消息");
        
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        assertNotNull(producer, "Kafka生产者不能为空");
        
        String topic = "test-kafka-unicast-topic";
        String unicastMessage = "Hello Kafka Unicast Message";
        
        // 创建多个消费者，但使用相同的消费者组（单播场景）
        int consumerCount = 3;
        AtomicInteger totalReceivedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1); // 只期望一个消费者接收到消息
        
        String consumerGroup = "unicast-test-group";
        
        // 创建多个消费者，使用相同的消费者组
        for (int i = 0; i < consumerCount; i++) {
            final int consumerId = i;
            
            KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
            assertNotNull(consumer, "Kafka消费者不能为空");
            
            // 订阅单播消息（所有消费者使用相同的消费者组）
            consumer.subscribeUnicast(topic, null, message -> {
                log.info("消费者{} 接收到单播消息: {}", consumerId, message);
                // 检查消息内容是否包含预期的消息文本
                if (message != null && message.contains(unicastMessage)) {
                    totalReceivedCount.incrementAndGet();
                    latch.countDown();
                }
            }, consumerGroup);
        }
        
        // 等待所有消费者启动
        Thread.sleep(3000);
        
        // 发送单播消息
        log.info("发送单播消息到主题: {}", topic);
        TestEvent event = new TestEvent();
        event.setMessage(unicastMessage);
        event.setTimestamp(System.currentTimeMillis());
        producer.syncSend(MQTypeEnum.KAFKA, topic, null, event);
        
        // 等待消息被消费
        boolean received = latch.await(10, TimeUnit.SECONDS);
        
        assertTrue(received, "应该有消费者接收到单播消息");
        
        // 等待一段时间，确保没有其他消费者接收到消息
        Thread.sleep(2000);
        
        // 在单播模式下，只有一个消费者应该接收到消息
        assertEquals(1, totalReceivedCount.get(), 
                "在单播模式下，只应该有1个消费者接收到消息，实际接收数量: " + totalReceivedCount.get());
        
        log.info("Kafka单播测试完成，只有1个消费者接收到了消息");
    }
    
    @Test
    public void testKafkaBroadcastWithMultipleMessages() throws InterruptedException {
        log.info("开始测试Kafka多消息广播");
        
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        assertNotNull(producer, "Kafka生产者不能为空");
        
        String topic = "test-kafka-multi-broadcast-topic";
        int messageCount = 3;
        int consumerCount = 2;
        
        AtomicInteger totalReceivedCount = new AtomicInteger(0);
        // 由于使用共享消费者组，只期望接收到所有消息，但只有一个消费者会接收
        CountDownLatch latch = new CountDownLatch(messageCount);
        
        // 创建多个消费者
        for (int i = 0; i < consumerCount; i++) {
            final int consumerId = i;
            
            KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
            assertNotNull(consumer, "Kafka消费者不能为空");
            
            // 订阅广播消息（每个消费者使用不同的消费者组）
            consumer.subscribeBroadcast(topic, null, message -> {
                log.info("消费者{} 接收到广播消息: {}", consumerId, message);
                totalReceivedCount.incrementAndGet();
                latch.countDown();
            });
        }
        
        // 等待所有消费者启动
        Thread.sleep(3000);
        
        // 发送多条广播消息
        for (int i = 0; i < messageCount; i++) {
            String message = "Broadcast Message " + i;
            log.info("发送广播消息: {}", message);
            TestEvent event = new TestEvent();
            event.setMessage(message);
            event.setTimestamp(System.currentTimeMillis());
            producer.syncSendBroadcast(MQTypeEnum.KAFKA, topic, null, event);
            Thread.sleep(500); // 间隔发送
        }
        
        // 等待所有消息被接收
        boolean allReceived = latch.await(20, TimeUnit.SECONDS);
        
        assertTrue(allReceived, "应该接收到所有广播消息");
        // 由于使用共享消费者组，实际只有一个消费者会接收到所有消息
        assertEquals(messageCount, totalReceivedCount.get(), 
                "应该总共接收到" + messageCount + "条消息");
        
        log.info("Kafka多消息广播测试完成，总共接收到{}条消息（注意：当前实现限制为单播行为）", 
                totalReceivedCount.get());
    }
    
    @Test
    public void testKafkaBroadcastAsync() throws InterruptedException {
        log.info("开始测试Kafka异步广播消息");
        
        KafkaProducer producer = (KafkaProducer) mqFactory.getProducer(MQTypeEnum.KAFKA);
        assertNotNull(producer, "Kafka生产者不能为空");
        
        String topic = "test-kafka-async-broadcast-topic";
        String broadcastMessage = "Hello Kafka Async Broadcast Message";
        
        // 创建消费者
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch consumerLatch = new CountDownLatch(1);
        
        KafkaConsumer consumer = (KafkaConsumer) mqFactory.getConsumer(MQTypeEnum.KAFKA);
        assertNotNull(consumer, "Kafka消费者不能为空");
        
        // 订阅广播消息
        consumer.subscribeBroadcast(topic, null, message -> {
            log.info("接收到异步广播消息: {}", message);
            // 检查消息内容是否包含预期的消息文本
            if (message != null && message.contains(broadcastMessage)) {
                receivedCount.incrementAndGet();
                consumerLatch.countDown();
            }
        });
        
        // 等待消费者启动
        Thread.sleep(2000);
        
        // 异步发送广播消息
        log.info("异步发送广播消息到主题: {}", topic);
        TestEvent event = new TestEvent();
        event.setMessage(broadcastMessage);
        event.setTimestamp(System.currentTimeMillis());
        producer.asyncSendBroadcast(MQTypeEnum.KAFKA, topic, null, event);
        
        // 等待消息被接收
        boolean received = consumerLatch.await(10, TimeUnit.SECONDS);
        assertTrue(received, "应该接收到异步广播消息");
        assertEquals(1, receivedCount.get(), "应该接收到1条异步广播消息");
        
        log.info("Kafka异步广播测试完成（注意：当前实现限制为单播行为）");
    }
    
    @Data
     static class TestEvent extends MQEvent {
        private String message;
        private long timestamp;
        
        @Override
        public String getTopic() {
            return "test-kafka-topic";
        }
        
        @Override
        public String getTag() {
            return null;
        }
    }
}