package com.lachesis.windrangerms.mq.deadletter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.consumer.MQConsumerManager;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试不同MQ类型的消息确认（ACK）机制
 * 注意：此测试需要配置相应的MQ服务才能运行
 */
@SpringBootTest
@ActiveProfiles("multi-mq-test") // 使用multi-mq-test配置文件
public class MultiMQAckTest {

    private static final Logger logger = LoggerFactory.getLogger(MultiMQAckTest.class);

    @Autowired
    private MQFactory mqFactory;

    private MQProducer mqProducer;

    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;

    @Autowired
    private MQConfig mqConfig;

    @Autowired
    private RabbitMQAckConsumer rabbitMQAckConsumer;

    @Autowired
    private RedisMQAckConsumer redisMQAckConsumer;

    @Autowired
    private MQConsumerManager mqConsumerManager;

    @BeforeEach
    public void setup() {
        // 不在这里初始化生产者，而是在每个测试方法中根据需要初始化
    }

    /**
     * 测试RabbitMQ消息确认机制
     */
    @Test
    public void testRabbitMQAck() throws InterruptedException {
        // 检查RabbitMQ是否启用
        if (mqConfig.getRabbitmq() == null || !mqConfig.getRabbitmq().isEnabled()) {
            logger.info("RabbitMQ未启用，跳过测试");
            return;
        }

        // 设置测试消费者为手动确认模式
        rabbitMQAckConsumer.setAckMode(MQAckConsumer.AckMode.MANUAL);

        // 发送测试消息
        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch processLatch = new CountDownLatch(1);
        rabbitMQAckConsumer.setProcessLatch(processLatch);

        CountDownLatch ackLatch = new CountDownLatch(1);
        rabbitMQAckConsumer.setAckLatch(ackLatch);

        logger.info("发送RabbitMQ测试消息，ID: {}", messageId);
        MQProducer rabbitMQProducer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        rabbitMQProducer.send("rabbit-ack-topic", "ack-tag", "这是一条需要手动确认的RabbitMQ测试消息", headers);

        // 等待消息处理
        boolean processAwait = processLatch.await(30, TimeUnit.SECONDS);
        assertTrue(processAwait, "消息处理超时");

        // 等待消息确认
        boolean ackAwait = ackLatch.await(30, TimeUnit.SECONDS);
        assertTrue(ackAwait, "消息确认超时");

        // 验证消息已被确认
        assertTrue(rabbitMQAckConsumer.isMessageAcknowledged(), "消息应该已被确认");
    }

    /**
     * 测试Redis消息确认机制
     */
    @Test
    public void testRedisMQAck() throws InterruptedException {
        // 检查Redis是否启用
        if (mqConfig.getRedis() == null || !mqConfig.getRedis().isEnabled()) {
            logger.info("Redis未启用，跳过测试");
            return;
        }

        // 设置测试消费者为手动确认模式
        redisMQAckConsumer.setAckMode(MQAckConsumer.AckMode.MANUAL);

        // 发送测试消息
        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch processLatch = new CountDownLatch(1);
        redisMQAckConsumer.setProcessLatch(processLatch);

        CountDownLatch ackLatch = new CountDownLatch(1);
        redisMQAckConsumer.setAckLatch(ackLatch);

        logger.info("发送Redis测试消息，ID: {}", messageId);
        MQProducer redisProducer = mqFactory.getProducer(MQTypeEnum.REDIS);
        redisProducer.send("redis-ack-topic", "ack-tag", "这是一条需要手动确认的Redis测试消息", headers);

        // 等待消息处理
        boolean processAwait = processLatch.await(30, TimeUnit.SECONDS);
        assertTrue(processAwait, "消息处理超时");

        // 等待消息确认
        boolean ackAwait = ackLatch.await(30, TimeUnit.SECONDS);
        assertTrue(ackAwait, "消息确认超时");

        // 验证消息已被确认
        assertTrue(redisMQAckConsumer.isMessageAcknowledged(), "消息应该已被确认");
    }

    /**
     * 测试RabbitMQ消息拒绝并进入死信队列
     */
    @Test
    public void testRabbitMQRejectToDeadLetter() throws InterruptedException {
        // 检查RabbitMQ是否启用
        if (mqConfig.getRabbitmq() == null || !mqConfig.getRabbitmq().isEnabled()) {
            logger.info("RabbitMQ未启用，跳过测试");
            return;
        }

        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        if (deadLetterService == null) {
            logger.info("死信队列服务未启用，跳过测试");
            return;
        }

        // 设置测试消费者为拒绝模式
        rabbitMQAckConsumer.setAckMode(MQAckConsumer.AckMode.REJECT);

        // 发送测试消息
        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch processLatch = new CountDownLatch(1);
        rabbitMQAckConsumer.setProcessLatch(processLatch);

        CountDownLatch rejectLatch = new CountDownLatch(1);
        rabbitMQAckConsumer.setRejectLatch(rejectLatch);

        logger.info("发送RabbitMQ测试消息（将被拒绝），ID: {}", messageId);
        MQProducer rabbitMQProducer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        rabbitMQProducer.send("rabbit-ack-topic", "reject-tag", "这是一条将被拒绝的RabbitMQ测试消息", headers);

        // 等待消息处理
        boolean processAwait = processLatch.await(30, TimeUnit.SECONDS);
        assertTrue(processAwait, "消息处理超时");

        // 等待消息拒绝
        boolean rejectAwait = rejectLatch.await(30, TimeUnit.SECONDS);
        assertTrue(rejectAwait, "消息拒绝超时");

        // 验证消息已被拒绝
        assertTrue(rabbitMQAckConsumer.isMessageRejected(), "消息应该已被拒绝");

        // 验证消息已进入死信队列
        // RabbitMQ消息需要重试3次才会进入死信队列，每次重试间隔可能较长
        Thread.sleep(10000); // 等待足够时间确保消息完成所有重试并进入死信队列

        List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
        boolean found = false;
        for (DeadLetterMessage message : messages) {
            if (message.getOriginalMessageId().equals(messageId) ||
                (message.getProperties() != null && messageId.equals(message.getProperties().get("messageId")))) {
                found = true;
                assertEquals("RABBIT_MQ", message.getMqType(), "MQ类型应该是RABBIT_MQ");
                logger.info("在死信队列中找到被拒绝的RabbitMQ消息: {}", JSON.toJSONString(message));
                break;
            }
        }

        assertTrue(found, "应该在死信队列中找到被拒绝的RabbitMQ消息");
    }

    /**
     * 从消息中提取消息ID的辅助方法
     */
    private static String extractMessageIdFromMessage(String message) {
        try {
            // 首先尝试解析整个消息为JSON
            JSONObject jsonObject = JSON.parseObject(message);
            String messageId = jsonObject.getString("id");
            if (messageId != null && !messageId.trim().isEmpty()) {
                return messageId;
            }
        } catch (Exception ignored) {
            // 解析失败，可能是Redis消息格式，尝试解析properties部分
        }
        
        // 处理Redis消息格式：properties JSON + "\n" + body
        try {
            if (message.contains("\n")) {
                String[] parts = message.split("\n", 2);
                if (parts.length >= 1) {
                    // 尝试解析第一部分为properties JSON
                    JSONObject properties = JSON.parseObject(parts[0]);
                    String messageId = properties.getString("messageId");
                    if (messageId != null && !messageId.trim().isEmpty()) {
                        return messageId;
                    }
                }
            }
        } catch (Exception ignored) {
            // 解析失败，忽略
        }
        
        // 使用消息内容的哈希值作为稳定的ID
        String stableId = String.valueOf(message.hashCode());
        if (stableId.startsWith("-")) {
            stableId = "0" + stableId.substring(1); // 移除负号
        }
        return stableId;
    }

    /**
     * MQ消息确认消费者接口
     */
    public interface MQAckConsumer {
        enum AckMode {
            MANUAL, REJECT
        }

        void setAckMode(AckMode mode);

        void setProcessLatch(CountDownLatch latch);

        void setAckLatch(CountDownLatch latch);

        void setRejectLatch(CountDownLatch latch);

        boolean isMessageAcknowledged();

        boolean isMessageRejected();
    }

    /**
     * RabbitMQ消息确认消费者
     */
    @Component
    public static class RabbitMQAckConsumer implements MQAckConsumer {

        private static final Logger logger = LoggerFactory.getLogger(RabbitMQAckConsumer.class);

        private AckMode ackMode = AckMode.MANUAL;
        private CountDownLatch processLatch;
        private CountDownLatch ackLatch;
        private CountDownLatch rejectLatch;
        private boolean messageAcknowledged = false;
        private boolean messageRejected = false;

        @MQConsumer(topic = "rabbit-ack-topic", tag = "ack-tag", mqType = MQTypeEnum.RABBIT_MQ)
        public void consumeAckMessage(String message) {
            String messageId = extractMessageIdFromMessage(message);
            logger.info("RabbitMQ Ack Consumer 接收到消息: {}, ID: {}", message, messageId);

            if (processLatch != null) {
                processLatch.countDown();
            }

            if (ackMode == AckMode.MANUAL) {
                // 模拟手动确认
                logger.info("RabbitMQ Ack Consumer 手动确认消息: {}", messageId);
                messageAcknowledged = true;
                if (ackLatch != null) {
                    ackLatch.countDown();
                }
            } else if (ackMode == AckMode.REJECT) {
                // 模拟拒绝消息
                logger.warn("RabbitMQ Ack Consumer 拒绝消息: {}", messageId);
                messageRejected = true;
                if (rejectLatch != null) {
                    rejectLatch.countDown();
                }
                throw new RuntimeException("模拟消息拒绝"); // 抛出异常以触发拒绝
            }
        }

        @MQConsumer(topic = "rabbit-ack-topic", tag = "reject-tag", mqType = MQTypeEnum.RABBIT_MQ)
        public void consumeRejectMessage(String message) {
            String messageId = extractMessageIdFromMessage(message);
            logger.info("RabbitMQ Reject Consumer 接收到消息: {}, ID: {}", message, messageId);

            if (processLatch != null) {
                processLatch.countDown();
            }

            // 对于reject-tag的消息，直接拒绝
            logger.warn("RabbitMQ Reject Consumer 拒绝消息: {}", messageId);
            messageRejected = true;
            if (rejectLatch != null) {
                rejectLatch.countDown();
            }
            throw new RuntimeException("模拟消息拒绝"); // 抛出异常以触发拒绝
        }

        @Override
        public void setAckMode(AckMode mode) {
            this.ackMode = mode;
        }

        @Override
        public void setProcessLatch(CountDownLatch latch) {
            this.processLatch = latch;
        }

        @Override
        public void setAckLatch(CountDownLatch latch) {
            this.ackLatch = latch;
        }

        @Override
        public void setRejectLatch(CountDownLatch latch) {
            this.rejectLatch = latch;
        }

        @Override
        public boolean isMessageAcknowledged() {
            return messageAcknowledged;
        }

        @Override
        public boolean isMessageRejected() {
            return messageRejected;
        }
    }

    /**
     * Redis消息确认消费者
     */
    @Component
    public static class RedisMQAckConsumer implements MQAckConsumer {

        private static final Logger logger = LoggerFactory.getLogger(RedisMQAckConsumer.class);

        private AckMode ackMode = AckMode.MANUAL;
        private CountDownLatch processLatch;
        private CountDownLatch ackLatch;
        private CountDownLatch rejectLatch;
        private boolean messageAcknowledged = false;
        private boolean messageRejected = false;

        @MQConsumer(topic = "redis-ack-topic", tag = "ack-tag", mqType = MQTypeEnum.REDIS)
        public void consumeAckMessage(String message) {
            String messageId = extractMessageIdFromMessage(message);
            logger.info("Redis Ack Consumer 接收到消息: {}, ID: {}", message, messageId);

            if (processLatch != null) {
                processLatch.countDown();
            }

            if (ackMode == AckMode.MANUAL) {
                // 模拟手动确认
                logger.info("Redis Ack Consumer 手动确认消息: {}", messageId);
                messageAcknowledged = true;
                if (ackLatch != null) {
                    ackLatch.countDown();
                }
            } else if (ackMode == AckMode.REJECT) {
                // 模拟拒绝消息
                logger.warn("Redis Ack Consumer 拒绝消息: {}", messageId);
                messageRejected = true;
                if (rejectLatch != null) {
                    rejectLatch.countDown();
                }
                throw new RuntimeException("模拟消息拒绝"); // 抛出异常以触发拒绝
            }
        }

        @Override
        public void setAckMode(AckMode mode) {
            this.ackMode = mode;
        }

        @Override
        public void setProcessLatch(CountDownLatch latch) {
            this.processLatch = latch;
        }

        @Override
        public void setAckLatch(CountDownLatch latch) {
            this.ackLatch = latch;
        }

        @Override
        public void setRejectLatch(CountDownLatch latch) {
            this.rejectLatch = latch;
        }

        @Override
        public boolean isMessageAcknowledged() {
            return messageAcknowledged;
        }

        @Override
        public boolean isMessageRejected() {
            return messageRejected;
        }
    }
}