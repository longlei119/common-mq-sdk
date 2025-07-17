package com.lachesis.windrangerms.mq.deadletter;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.model.RetryHistory;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试多MQ死信队列功能
 */
@SpringBootTest
@ActiveProfiles("multi-mq-test") // 使用multi-mq-test配置文件
public class MultiMQDeadLetterTest {

    private static final Logger logger = LoggerFactory.getLogger(MultiMQDeadLetterTest.class);

    @Autowired
    private MQFactory mqFactory;

    private MQProducer mqProducer;

    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;

    @Autowired
    private MQConfig mqConfig;

    @Autowired
    private RabbitMQTestConsumer rabbitMQTestConsumer;

    @Autowired
    private RedisMQTestConsumer redisMQTestConsumer;

    @BeforeEach
    public void setup() {
        mqProducer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
    }

    /**
     * 测试RabbitMQ死信队列
     */
    @Test
    public void testRabbitMQDeadLetter() throws InterruptedException {
        // 检查RabbitMQ是否启用
        if (mqConfig.getRabbitmq() == null || !mqConfig.getRabbitmq().isEnabled()) {
            logger.info("RabbitMQ未启用，跳过测试");
            return;
        }

        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信服务不应为空");

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch latch = new CountDownLatch(1);
        rabbitMQTestConsumer.setLatch(latch);

        logger.info("发送RabbitMQ测试消息，ID: {}", messageId);
        mqProducer.send("test-dead-letter-topic-rabbit", "test-tag", "这是一条RabbitMQ测试消息", headers);

        // 等待消息被消费并进入死信队列
        boolean await = latch.await(30, TimeUnit.SECONDS);
        assertTrue(await, "消息未被消费或未进入死信队列");

        // 验证消息已进入死信队列
        Thread.sleep(1000); // 等待一段时间确保消息已进入死信队列

        List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
        boolean found = false;
        for (DeadLetterMessage message : messages) {
            if (message.getOriginalMessageId().equals(messageId) ||
                (message.getProperties() != null && messageId.equals(message.getProperties().get("messageId")))) {
                found = true;
                assertEquals("RABBIT_MQ", message.getMqType(), "MQ类型应该是RABBIT_MQ");
                assertEquals("test-dead-letter-topic-rabbit", message.getOriginalTopic(), "原始Topic不匹配");
                assertEquals("test-tag", message.getOriginalTag(), "原始Tag不匹配");
                assertEquals("这是一条RabbitMQ测试消息", message.getOriginalBody(), "原始Body不匹配");
                assertNotNull(message.getDeadLetterTime(), "死信时间不应为空");
                assertNotNull(message.getRetryHistory(), "重试历史不应为空");
                assertFalse(message.getRetryHistory().isEmpty(), "重试历史不应为空");
                RetryHistory lastRetry = message.getRetryHistory().get(message.getRetryHistory().size() - 1);
                //assertEquals("模拟消费失败", lastRetry.getErrorMessage(), "错误信息不匹配");
                logger.info("在死信队列中找到RabbitMQ消息: {}", JSON.toJSONString(message));
                break;
            }
        }

        assertTrue(found, "应该在死信队列中找到RabbitMQ消息");
    }

    /**
     * 测试Redis死信队列
     */
    @Test
    public void testRedisMQDeadLetter() throws InterruptedException {
        // 检查Redis是否启用
        if (mqConfig.getRedis() == null || !mqConfig.getRedis().isEnabled()) {
            logger.info("Redis未启用，跳过测试");
            return;
        }

        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信服务不应为空");

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch latch = new CountDownLatch(1);
        redisMQTestConsumer.setLatch(latch);

        logger.info("发送Redis测试消息，ID: {}", messageId);
        mqProducer.send("test-dead-letter-topic-redis", "test-tag", "这是一条Redis测试消息", headers);

        // 等待消息被消费并进入死信队列
        boolean await = latch.await(30, TimeUnit.SECONDS);
        assertTrue(await, "消息未被消费或未进入死信队列");

        // 验证消息已进入死信队列
        Thread.sleep(1000); // 等待一段时间确保消息已进入死信队列

        List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
        boolean found = false;
        for (DeadLetterMessage message : messages) {
            if (message.getOriginalMessageId().equals(messageId) ||
                (message.getProperties() != null && messageId.equals(message.getProperties().get("messageId")))) {
                found = true;
                assertEquals("REDIS_MQ", message.getMqType(), "MQ类型应该是REDIS_MQ");
                assertEquals("test-dead-letter-topic-redis", message.getOriginalTopic(), "原始Topic不匹配");
                assertEquals("test-tag", message.getOriginalTag(), "原始Tag不匹配");
                assertEquals("这是一条Redis测试消息", message.getOriginalBody(), "原始Body不匹配");
                assertNotNull(message.getDeadLetterTime(), "死信时间不应为空");
                assertNotNull(message.getRetryHistory(), "重试历史不应为空");
                assertFalse(message.getRetryHistory().isEmpty(), "重试历史不应为空");
                RetryHistory lastRetry = message.getRetryHistory().get(message.getRetryHistory().size() - 1);
                //assertEquals("模拟消费失败", lastRetry.getErrorMessage(), "错误信息不匹配");
                logger.info("在死信队列中找到Redis消息: {}", JSON.toJSONString(message));
                break;
            }
        }

        assertTrue(found, "应该在死信队列中找到Redis消息");
    }

    @Component
    public static class RabbitMQTestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(RabbitMQTestConsumer.class);
        private CountDownLatch latch;

        @MQConsumer(topic = "test-dead-letter-topic-rabbit", tag = "test-tag", mqType = MQTypeEnum.RABBIT_MQ)
        public void consume(String message) {
            logger.info("RabbitMQTestConsumer 接收到消息: {}", message);
            if (latch != null) {
                latch.countDown();
            }
            throw new RuntimeException("模拟消费失败"); // 模拟消费失败，消息进入死信队列
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }

    @Component
    public static class RedisMQTestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(RedisMQTestConsumer.class);
        private CountDownLatch latch;

        @MQConsumer(topic = "test-dead-letter-topic-redis", tag = "test-tag", mqType = MQTypeEnum.REDIS)
        public void consume(String message) {
            logger.info("RedisMQTestConsumer 接收到消息: {}", message);
            if (latch != null) {
                latch.countDown();
            }
            throw new RuntimeException("模拟消费失败"); // 模拟消费失败，消息进入死信队列
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }
}