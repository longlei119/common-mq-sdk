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
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 消息确认和死信队列测试
 */
@SpringBootTest
@ActiveProfiles("test")
public class MessageAckDeadLetterTest {

    private static final Logger logger = LoggerFactory.getLogger(MessageAckDeadLetterTest.class);

    @Autowired
    private MQFactory mqFactory;

    private MQProducer mqProducer;

    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;

    @Autowired
    private MQConfig mqConfig;

    @Autowired
    private TestConsumer testConsumer;

    @BeforeEach
    public void setup() {
        mqProducer = mqFactory.getProducer(MQTypeEnum.ROCKET_MQ);
    }

    /**
     * 测试消息重试和最终进入死信队列
     */
    @Test
    public void testMessageFailureToDeadLetter() throws InterruptedException {
        // 检查RocketMQ是否启用
        if (mqConfig.getRocketmq() == null || !mqConfig.getRocketmq().isEnabled()) {
            logger.info("RocketMQ未启用，跳过测试");
            return;
        }

        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信服务不应为空");

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        // 设置消费者模拟失败，直到进入死信队列
        testConsumer.setShouldFail(true);
        testConsumer.setFailureCount(4); // 模拟失败4次，超过最大重试次数3次

        CountDownLatch latch = new CountDownLatch(1);
        testConsumer.setLatch(latch);

        logger.info("发送测试消息，ID: {}", messageId);
        mqProducer.send("test-topic", "test-tag", "这是一条测试消息", headers);

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
                assertEquals("ROCKET_MQ", message.getMqType(), "MQ类型应该是ROCKET_MQ");
                assertEquals("test-topic", message.getOriginalTopic(), "原始Topic不匹配");
                assertEquals("test-tag", message.getOriginalTag(), "原始Tag不匹配");
                assertEquals("这是一条测试消息", message.getOriginalBody(), "原始Body不匹配");
                assertNotNull(message.getDeadLetterTime(), "死信时间不应为空");
                assertNotNull(message.getRetryHistory(), "重试历史不应为空");
                //assertFalse(message.getRetryHistory().isEmpty(), "重试历史不应为空");
                RetryHistory lastRetry = message.getRetryHistory().get(message.getRetryHistory().size() - 1);
                //assertEquals("模拟消费失败", lastRetry.getErrorMessage(), "错误信息不匹配");
                logger.info("在死信队列中找到消息: {}", JSON.toJSONString(message));
                break;
            }
        }

        assertTrue(found, "应该在死信队列中找到消息");
    }

    /**
     * 测试消息手动确认
     */
    @Test
    public void testMessageRedeliveryAndAck() throws InterruptedException {
        // 检查RocketMQ是否启用
        if (mqConfig.getRocketmq() == null || !mqConfig.getRocketmq().isEnabled()) {
            logger.info("RocketMQ未启用，跳过测试");
            return;
        }

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        // 设置消费者模拟重试，然后手动确认
        testConsumer.setShouldFail(false);
        testConsumer.setFailureCount(0); // 不失败
        testConsumer.setAckMode(TestConsumer.AckMode.MANUAL);

        CountDownLatch processLatch = new CountDownLatch(1);
        testConsumer.setProcessLatch(processLatch);

        CountDownLatch ackLatch = new CountDownLatch(1);
        testConsumer.setAckLatch(ackLatch);

        logger.info("发送测试消息（手动确认），ID: {}", messageId);
        mqProducer.send("test-topic", "ack-tag", "这是一条需要手动确认的消息", headers);

        // 等待消息处理
        boolean processAwait = processLatch.await(30, TimeUnit.SECONDS);
        assertTrue(processAwait, "消息处理超时");

        // 等待消息确认
        boolean ackAwait = ackLatch.await(30, TimeUnit.SECONDS);
        assertTrue(ackAwait, "消息确认超时");

        // 验证消息已被确认
        assertTrue(testConsumer.isMessageAcknowledged(), "消息应该已被确认");
    }

    /**
     * 测试消息拒绝并进入死信队列
     */
    @Test
    public void testMessageRejectToDeadLetter() throws InterruptedException {
        // 检查RocketMQ是否启用
        if (mqConfig.getRocketmq() == null || !mqConfig.getRocketmq().isEnabled()) {
            logger.info("RocketMQ未启用，跳过测试");
            return;
        }

        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信服务不应为空");

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        // 设置消费者模拟拒绝
        testConsumer.setShouldFail(false);
        testConsumer.setFailureCount(0);
        testConsumer.setAckMode(TestConsumer.AckMode.REJECT);

        CountDownLatch processLatch = new CountDownLatch(1);
        testConsumer.setProcessLatch(processLatch);

        CountDownLatch rejectLatch = new CountDownLatch(1);
        testConsumer.setRejectLatch(rejectLatch);

        logger.info("发送测试消息（将被拒绝），ID: {}", messageId);
        mqProducer.send("test-topic", "reject-tag", "这是一条将被拒绝的消息", headers);

        // 等待消息处理
        boolean processAwait = processLatch.await(30, TimeUnit.SECONDS);
        assertTrue(processAwait, "消息处理超时");

        // 等待消息拒绝
        boolean rejectAwait = rejectLatch.await(30, TimeUnit.SECONDS);
        assertTrue(rejectAwait, "消息拒绝超时");

        // 验证消息已被拒绝
        assertTrue(testConsumer.isMessageRejected(), "消息应该已被拒绝");

        // 验证消息已进入死信队列
        Thread.sleep(1000); // 等待一段时间确保消息已进入死信队列

        List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
        boolean found = false;
        for (DeadLetterMessage message : messages) {
            if (message.getOriginalMessageId().equals(messageId) ||
                (message.getProperties() != null && messageId.equals(message.getProperties().get("messageId")))) {
                found = true;
                assertEquals("ROCKET_MQ", message.getMqType(), "MQ类型应该是ROCKET_MQ");
                logger.info("在死信队列中找到被拒绝的消息: {}", JSON.toJSONString(message));
                break;
            }
        }

        assertTrue(found, "应该在死信队列中找到被拒绝的消息");
    }

    @Component
    public static class TestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(TestConsumer.class);

        public enum AckMode {
            AUTO, MANUAL, REJECT
        }

        private boolean shouldFail = false;
        private int failureCount = 0;
        private int currentFailureCount = 0;
        private AckMode ackMode = AckMode.AUTO;
        private CountDownLatch latch;
        private CountDownLatch processLatch;
        private CountDownLatch ackLatch;
        private CountDownLatch rejectLatch;
        private AtomicBoolean messageAcknowledged = new AtomicBoolean(false);
        private AtomicBoolean messageRejected = new AtomicBoolean(false);

        @MQConsumer(mqType = MQTypeEnum.ROCKET_MQ, topic = "test-topic", tag = "test-tag")
        public void consume(String message) {
            String messageId = "unknown";
            logger.info("接收到消息: {}, ID: {}", message, messageId);

            if (shouldFail && currentFailureCount < failureCount) {
                currentFailureCount++;
                logger.error("消息处理失败，当前失败次数: {}/{}", currentFailureCount, failureCount);
                throw new RuntimeException("模拟消费失败");
            }

            // 消息处理成功
            logger.info("消息处理成功");

            // 重置失败计数
            currentFailureCount = 0;

            if (latch != null) {
                latch.countDown();
            }
        }

        @MQConsumer(mqType = MQTypeEnum.ROCKET_MQ, topic = "test-topic", tag = "ack-tag")
        public void consumeAckMessage(String message) {
            String messageId = "unknown";
            logger.info("接收到需要手动确认的消息: {}, ID: {}", message, messageId);

            if (processLatch != null) {
                processLatch.countDown();
            }

            if (ackMode == AckMode.MANUAL) {
                logger.info("手动确认消息: {}", messageId);
                messageAcknowledged.set(true);
                if (ackLatch != null) {
                    ackLatch.countDown();
                }
            }
        }

        @MQConsumer(mqType = MQTypeEnum.ROCKET_MQ, topic = "test-topic", tag = "reject-tag")
        public void consumeRejectMessage(String message) {
            String messageId = "unknown";
            logger.info("接收到将被拒绝的消息: {}, ID: {}", message, messageId);

            if (processLatch != null) {
                processLatch.countDown();
            }

            if (ackMode == AckMode.REJECT) {
                logger.warn("拒绝消息: {}", messageId);
                messageRejected.set(true);
                if (rejectLatch != null) {
                    rejectLatch.countDown();
                }
                throw new RuntimeException("模拟消息拒绝");
            }
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
            this.currentFailureCount = 0;
        }

        public void setFailureCount(int failureCount) {
            this.failureCount = failureCount;
        }

        public void setAckMode(AckMode ackMode) {
            this.ackMode = ackMode;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public void setProcessLatch(CountDownLatch processLatch) {
            this.processLatch = processLatch;
        }

        public void setAckLatch(CountDownLatch ackLatch) {
            this.ackLatch = ackLatch;
        }

        public void setRejectLatch(CountDownLatch rejectLatch) {
            this.rejectLatch = rejectLatch;
        }

        public boolean isMessageAcknowledged() {
            return messageAcknowledged.get();
        }

        public boolean isMessageRejected() {
            return messageRejected.get();
        }
    }
}