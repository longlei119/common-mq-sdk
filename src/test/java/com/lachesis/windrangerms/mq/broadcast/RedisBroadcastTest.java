package com.lachesis.windrangerms.mq.broadcast;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis广播模式测试
 */
@Slf4j
@SpringBootTest
@TestPropertySource(properties = {
    "mq.redis.host=localhost",
    "mq.redis.port=6379",
    "mq.redis.database=0"
})
public class RedisBroadcastTest {

    @Autowired
    private MQFactory mqFactory;
    
    /**
     * 测试用的MQEvent实现类
     */
    public static class TestMQEvent extends MQEvent {
        private String eventType;
        private String data;
        private String topic;
        private String tag;
        
        public TestMQEvent(String topic, String tag) {
            this.topic = topic;
            this.tag = tag;
        }
        
        @Override
        public String getTopic() {
            return topic;
        }
        
        @Override
        public String getTag() {
            return tag;
        }
        
        public String getEventType() {
            return eventType;
        }
        
        public void setEventType(String eventType) {
            this.eventType = eventType;
        }
        
        public String getData() {
            return data;
        }
        
        public void setData(String data) {
            this.data = data;
        }
    }

    @Test
    public void testRedisBroadcast() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.REDIS);
        
        String topic = "test-broadcast-topic";
        String tag = "test-tag";
        
        // 创建多个消费者来测试广播
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);
        AtomicInteger consumer3Count = new AtomicInteger(0);
        
        CountDownLatch latch = new CountDownLatch(3); // 期望3个消费者都收到消息
        
        // 消费者1
        consumer.subscribeBroadcast(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("消费者1收到广播消息: {}", message);
            consumer1Count.incrementAndGet();
            latch.countDown();
        });
        
        // 消费者2
        consumer.subscribeBroadcast(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("消费者2收到广播消息: {}", message);
            consumer2Count.incrementAndGet();
            latch.countDown();
        });
        
        // 消费者3
        consumer.subscribeBroadcast(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("消费者3收到广播消息: {}", message);
            consumer3Count.incrementAndGet();
            latch.countDown();
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送广播消息
        TestMQEvent event = new TestMQEvent(topic, tag);
        event.setEventType("BROADCAST_TEST");
        event.setData("这是一条广播测试消息");
        
        String messageId = producer.syncSendBroadcast(MQTypeEnum.REDIS, topic, tag, event);
        log.info("发送广播消息成功，messageId: {}", messageId);
        
        // 等待消息处理
        boolean success = latch.await(10, TimeUnit.SECONDS);
        
        log.info("广播测试结果:");
        log.info("消费者1收到消息数量: {}", consumer1Count.get());
        log.info("消费者2收到消息数量: {}", consumer2Count.get());
        log.info("消费者3收到消息数量: {}", consumer3Count.get());
        log.info("所有消费者是否都收到消息: {}", success);
        
        // 验证每个消费者都收到了消息
        assert consumer1Count.get() == 1 : "消费者1应该收到1条消息";
        assert consumer2Count.get() == 1 : "消费者2应该收到1条消息";
        assert consumer3Count.get() == 1 : "消费者3应该收到1条消息";
        assert success : "所有消费者都应该收到消息";
        
        // 清理订阅
        consumer.unsubscribeBroadcast(MQTypeEnum.REDIS, topic, tag);
        
        log.info("Redis广播模式测试通过！");
    }
    
    /**
     * 测试Redis单播与广播模式的区别
     * 注意：Redis单播模式每次订阅会覆盖之前的订阅，所以只有最后一个消费者会收到消息
     * 而广播模式支持多个消费者同时接收消息
     */
    @Test
    public void testRedisUnicastVsBroadcast() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.REDIS);
        
        String topic = "test-compare-topic";
        String tag = "test-tag";
        
        // 单播模式测试
        AtomicInteger unicastCount = new AtomicInteger(0);
        CountDownLatch unicastLatch = new CountDownLatch(1); // Redis单播只有最后一个消费者会收到消息
        
        // 创建两个单播消费者（Redis中只有最后一个会收到消息）
        log.info("开始订阅单播消费者1");
        consumer.subscribe(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("单播消费者1收到消息: {}", message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        // 等待第一个订阅生效
        Thread.sleep(500);
        
        log.info("开始订阅单播消费者2");
        consumer.subscribe(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("单播消费者2收到消息: {}", message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        // 等待第二个订阅生效，确保第一个监听器已被移除
        Thread.sleep(1500);
        
        // 发送单播消息
        TestMQEvent unicastEvent = new TestMQEvent(topic, tag);
        unicastEvent.setEventType("UNICAST_TEST");
        unicastEvent.setData("这是一条单播测试消息");
        
        producer.syncSend(MQTypeEnum.REDIS, topic, tag, unicastEvent);
        unicastLatch.await(5, TimeUnit.SECONDS);
        
        log.info("单播模式收到消息总数: {}", unicastCount.get());
        
        // 广播模式测试
        AtomicInteger broadcastCount = new AtomicInteger(0);
        CountDownLatch broadcastLatch = new CountDownLatch(2);
        
        // 创建两个广播消费者（都会收到消息）
        consumer.subscribeBroadcast(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("广播消费者1收到消息: {}", message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        consumer.subscribeBroadcast(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("广播消费者2收到消息: {}", message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        Thread.sleep(1000);
        
        // 发送广播消息
        TestMQEvent broadcastEvent = new TestMQEvent(topic, tag);
        broadcastEvent.setEventType("BROADCAST_TEST");
        broadcastEvent.setData("这是一条广播测试消息");
        
        producer.syncSendBroadcast(MQTypeEnum.REDIS, topic, tag, broadcastEvent);
        boolean broadcastSuccess = broadcastLatch.await(5, TimeUnit.SECONDS);
        
        log.info("广播模式收到消息总数: {}", broadcastCount.get());
        log.info("广播模式是否成功: {}", broadcastSuccess);
        
        // 验证结果
        // Redis单播模式每次订阅会覆盖之前的订阅，所以只有最后一个消费者会收到消息
        assert unicastCount.get() == 1 : "Redis单播模式只有最后一个消费者会收到消息，实际收到: " + unicastCount.get();
        assert broadcastCount.get() == 2 : "广播模式应该有2个消费者都收到消息，实际收到: " + broadcastCount.get();
        assert broadcastSuccess : "广播模式应该成功";
        
        // 清理订阅
        consumer.unsubscribe(MQTypeEnum.REDIS, topic, tag);
        consumer.unsubscribeBroadcast(MQTypeEnum.REDIS, topic, tag);
        
        log.info("Redis单播vs广播对比测试通过！");
    }
    
    @Test
    public void testRedisAsyncBroadcast() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.REDIS);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.REDIS);
        
        String topic = "test-async-broadcast";
        String tag = "async-tag";
        
        AtomicInteger messageCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);
        
        // 创建两个消费者
        consumer.subscribeBroadcast(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("异步广播消费者1收到消息: {}", message);
            messageCount.incrementAndGet();
            latch.countDown();
        });
        
        consumer.subscribeBroadcast(MQTypeEnum.REDIS, topic, tag, message -> {
            log.info("异步广播消费者2收到消息: {}", message);
            messageCount.incrementAndGet();
            latch.countDown();
        });
        
        Thread.sleep(1000);
        
        // 异步发送广播消息
        producer.asyncSendBroadcast(MQTypeEnum.REDIS, topic, tag, "异步广播测试消息");
        
        boolean success = latch.await(10, TimeUnit.SECONDS);
        
        log.info("异步广播测试结果: 收到消息数量={}, 是否成功={}", messageCount.get(), success);
        
        assert messageCount.get() == 2 : "应该有2个消费者收到异步广播消息";
        assert success : "异步广播应该成功";
        
        // 清理订阅
        consumer.unsubscribeBroadcast(MQTypeEnum.REDIS, topic, tag);
        
        log.info("Redis异步广播测试通过！");
    }
}