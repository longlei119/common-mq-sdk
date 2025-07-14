package com.example.mq.broadcast;

import com.example.mq.consumer.MQConsumer;
import com.example.mq.model.MQEvent;
import com.example.mq.factory.MQFactory;
import com.example.mq.producer.MQProducer;
import com.example.mq.enums.MQTypeEnum;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RabbitMQ广播模式测试
 */
@Slf4j
@SpringBootTest
@TestPropertySource(properties = {
    "mq.rabbitmq.enabled=true",
    "mq.redis.enabled=true",
    "mq.delay.enabled=true"
})
public class RabbitMQBroadcastTest {

    @Autowired
    private MQFactory mqFactory;
    
    private static final String TOPIC = "real_test_topic";
    private static final String TAG = "real_test_tag";

    @Test
    @ConditionalOnProperty(name = "mq.rabbitmq.enabled", havingValue = "true")
    void testRabbitMQUnicastVsBroadcast() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        // 单播模式测试
        AtomicInteger unicastCount = new AtomicInteger(0);
        CountDownLatch unicastLatch = new CountDownLatch(1); // RabbitMQ单播模式应该只有一个消费者收到消息
        
        // 创建两个单播消费者（实际上是同一个消费者实例的多次订阅，会覆盖）
        log.info("开始订阅单播消费者1");
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC + ".unicast", TAG, (message) -> {
            log.info("单播消费者1收到消息: {}", message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        log.info("开始订阅单播消费者2");
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC + ".unicast", TAG, (message) -> {
            log.info("单播消费者2收到消息: {}", message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送单播消息
        TestEvent unicastEvent = new TestEvent();
        unicastEvent.setMessage("RabbitMQ单播测试消息");
        unicastEvent.setTimestamp(System.currentTimeMillis());
        unicastEvent.setSequence(1);
        unicastEvent.setTraceId("unicast-test-001");
        
        log.info("发送单播消息");
        producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC + ".unicast", TAG, unicastEvent);
        
        // 等待消息处理
        assertTrue(unicastLatch.await(10, TimeUnit.SECONDS), "单播消息接收超时");
        
        // 验证单播模式：同一个消费者实例的多次订阅会覆盖，所以只有最后一个订阅生效
        assertEquals(1, unicastCount.get(), "RabbitMQ单播模式只有最后一个消费者会收到消息，实际收到: " + unicastCount.get());
        
        log.info("RabbitMQ单播模式测试完成，收到消息数: {}", unicastCount.get());
        
        // 广播模式测试
        MQConsumer consumer2 = mqFactory.getConsumer(MQTypeEnum.RABBIT_MQ);
        
        AtomicInteger broadcastCount = new AtomicInteger(0);
        CountDownLatch broadcastLatch = new CountDownLatch(2); // 广播模式期望2个消费者都收到消息
        
        log.info("开始订阅广播消费者1");
        consumer.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC + ".broadcast", TAG, (message) -> {
            log.info("广播消费者1收到消息: {}", message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        log.info("开始订阅广播消费者2");
        consumer2.subscribe(MQTypeEnum.RABBIT_MQ, TOPIC + ".broadcast", TAG, (message) -> {
            log.info("广播消费者2收到消息: {}", message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        // 等待广播订阅生效
        Thread.sleep(1000);
        
        // 发送广播消息
        TestEvent broadcastEvent = new TestEvent();
        broadcastEvent.setMessage("RabbitMQ广播测试消息");
        broadcastEvent.setTimestamp(System.currentTimeMillis());
        broadcastEvent.setSequence(2);
        broadcastEvent.setTraceId("broadcast-test-001");
        
        log.info("发送广播消息");
        producer.syncSend(MQTypeEnum.RABBIT_MQ, TOPIC + ".broadcast", TAG, broadcastEvent);
        
        // 等待广播消息处理
        assertTrue(broadcastLatch.await(10, TimeUnit.SECONDS), "广播消息接收超时");
        
        // 验证广播模式：不同的消费者实例都应该收到消息
        assertEquals(2, broadcastCount.get(), "RabbitMQ广播模式所有消费者都应该收到消息，实际收到: " + broadcastCount.get());
        
        log.info("RabbitMQ广播模式测试完成，收到消息数: {}", broadcastCount.get());
        log.info("RabbitMQ单播vs广播测试完成");
    }

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
}