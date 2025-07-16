package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RocketMQ简单测试
 */
@Slf4j
@SpringBootTest
public class RocketMQSimpleTest {

    @Autowired
    private MQFactory mqFactory;

    @Test
    @ConditionalOnProperty(name = "mq.rocketmq.enabled", havingValue = "true")
    void testRocketMQSimple() throws InterruptedException {
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ);
        
        String topic = "simple-test-topic";
        String tag = "simple-tag";
        
        AtomicInteger messageCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        
        // 订阅消息
        log.info("开始订阅消息");
        consumer.subscribe(MQTypeEnum.ROCKET_MQ, topic, tag, message -> {
            log.info("收到消息: {}", message);
            messageCount.incrementAndGet();
            latch.countDown();
        });
        
        // 等待订阅生效
        Thread.sleep(2000);
        
        // 发送消息
        TestEvent event = new TestEvent();
        event.setMessage("简单测试消息");
        event.setTimestamp(System.currentTimeMillis());
        
        log.info("发送消息");
        String msgId = producer.syncSend(MQTypeEnum.ROCKET_MQ, topic, tag, event);
        log.info("消息发送完成，消息ID: {}", msgId);
        
        // 等待消息处理
        boolean received = latch.await(15, TimeUnit.SECONDS);
        log.info("消息接收结果: {}, 收到消息数: {}", received, messageCount.get());
        
        assertTrue(received, "消息接收超时");
        assertTrue(messageCount.get() > 0, "没有收到任何消息");
        
        log.info("RocketMQ简单测试完成");
    }

    @Data
    static class TestEvent extends MQEvent {
        private String message;
        private long timestamp;

        @Override
        public String getTopic() {
            return "simple-test-topic";
        }

        @Override
        public String getTag() {
            return "simple-tag";
        }
    }
}