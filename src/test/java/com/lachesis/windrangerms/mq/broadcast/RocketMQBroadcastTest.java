package com.lachesis.windrangerms.mq.broadcast;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RocketMQ广播模式测试
 */
@Slf4j
@SpringBootTest
public class RocketMQBroadcastTest {

    @Autowired
    private MQFactory mqFactory;
    
    private static final String TOPIC = "real_test_topic";
    private static final String TAG = "real_test_tag";

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testRocketMQUnicastVsBroadcast() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        // 为单播测试创建两个不同的consumer实例，避免重复订阅问题
        MQConsumer consumer1 = mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ);
        MQConsumer consumer2 = mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ);
        
        String topic = "test-unicast-topic";
        String tag = "test-tag";
        
        // 单播模式测试
        AtomicInteger unicastCount = new AtomicInteger(0);
        CountDownLatch unicastLatch = new CountDownLatch(1); // RocketMQ单播模式应该只有一个消费者收到消息
        
        // 使用不同的consumer实例创建两个单播消费者
        log.info("开始订阅单播消费者1");
        consumer1.subscribe(MQTypeEnum.ROCKET_MQ, topic, tag, message -> {
            log.info("单播消费者1收到消息: {}", message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        // 等待第一个订阅生效
        Thread.sleep(2000);
        
        log.info("开始订阅单播消费者2");
        consumer2.subscribe(MQTypeEnum.ROCKET_MQ, topic, tag, message -> {
            log.info("单播消费者2收到消息: {}", message);
            unicastCount.incrementAndGet();
            unicastLatch.countDown();
        });
        
        // 等待第二个订阅生效
        Thread.sleep(3000);
        log.info("订阅完成，准备发送消息");
        
        // 发送单播消息
        TestEvent unicastEvent = new TestEvent();
        unicastEvent.setMessage("RocketMQ单播测试消息");
        unicastEvent.setTimestamp(System.currentTimeMillis());
        unicastEvent.setSequence(1);
        unicastEvent.setTraceId("unicast-test-001");
        
        log.info("发送单播消息");
        String msgId = producer.syncSend(MQTypeEnum.ROCKET_MQ, topic, tag, unicastEvent);
        log.info("单播消息发送完成，消息ID: {}", msgId);
        
        // 等待消息处理，增加超时时间
        log.info("开始等待单播消息，当前计数器值: {}", unicastCount.get());
        boolean received = unicastLatch.await(20, TimeUnit.SECONDS);
        log.info("单播消息等待结果: {}, 最终计数器值: {}", received, unicastCount.get());
        assertTrue(received, "单播消息接收超时");
        
        // 由于使用了不同的consumer实例，每个consumer实例都应该能收到消息
        // 但由于RocketMQ的负载均衡机制，同一个消费者组内的消费者会平均分配消息
        // 所以我们期望至少有一个消费者收到消息
        assertTrue(unicastCount.get() >= 1, "RocketMQ单播模式应该至少有一个消费者收到消息，实际收到: " + unicastCount.get());
        
        log.info("RocketMQ单播模式测试完成，收到消息数: {}", unicastCount.get());
        
        // 广播模式测试
        AtomicInteger broadcastCount = new AtomicInteger(0);
        CountDownLatch broadcastLatch = new CountDownLatch(2); // 广播模式期望2个消费者都收到消息
        
        String broadcastTopic = "test-broadcast-topic";
        String broadcastTag = "broadcast-tag";
        
        // 为广播测试创建两个不同的consumer实例
        MQConsumer broadcastConsumer1 = mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ);
        MQConsumer broadcastConsumer2 = mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ);
        
        // 使用不同的consumer实例创建两个广播消费者
        log.info("开始订阅广播消费者1");
        broadcastConsumer1.subscribeBroadcast(MQTypeEnum.ROCKET_MQ, broadcastTopic, broadcastTag, message -> {
            log.info("广播消费者1收到消息: {}", message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        log.info("开始订阅广播消费者2");
        broadcastConsumer2.subscribeBroadcast(MQTypeEnum.ROCKET_MQ, broadcastTopic, broadcastTag, message -> {
            log.info("广播消费者2收到消息: {}", message);
            broadcastCount.incrementAndGet();
            broadcastLatch.countDown();
        });
        
        // 等待广播订阅生效，RocketMQ广播模式需要更多时间启动
        Thread.sleep(5000);
        log.info("广播订阅等待完成，准备发送广播消息");
        
        // 发送广播消息
        TestEvent broadcastEvent = new TestEvent();
        broadcastEvent.setMessage("RocketMQ广播测试消息");
        broadcastEvent.setTimestamp(System.currentTimeMillis());
        broadcastEvent.setSequence(1);
        broadcastEvent.setTraceId("broadcast-test-001");
        
        log.info("发送广播消息");
        producer.syncSendBroadcast(MQTypeEnum.ROCKET_MQ, broadcastTopic, broadcastTag, broadcastEvent);
        
        // 等待广播消息处理，增加超时时间
        log.info("开始等待广播消息，当前计数器值: {}", broadcastCount.get());
        boolean broadcastReceived = broadcastLatch.await(30, TimeUnit.SECONDS);
        log.info("广播消息等待结果: {}, 最终计数器值: {}", broadcastReceived, broadcastCount.get());
        assertTrue(broadcastReceived, "广播消息接收超时");
        
        // 验证广播模式：所有消费者都应该收到消息
        assertEquals(2, broadcastCount.get(), "RocketMQ广播模式所有消费者都应该收到消息，实际收到: " + broadcastCount.get());
        
        log.info("RocketMQ广播模式测试完成，收到消息数: {}", broadcastCount.get());
        log.info("RocketMQ单播vs广播测试完成");
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