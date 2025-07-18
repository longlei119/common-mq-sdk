package com.lachesis.windrangerms.mq.deadletter;

import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.enums.MessageMode;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试死信队列的批量操作功能
 */
@SpringBootTest
@TestPropertySource(locations = "classpath:application-batch-test.yml")
public class DeadLetterBatchOperationTest {

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterBatchOperationTest.class);

    @Autowired
    private MQFactory mqFactory;

    private MQProducer mqProducer;

    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;

    @Autowired
    private BatchTestConsumer batchTestConsumer;

    private DeadLetterService deadLetterService;

    @BeforeEach
    public void setup() {
        mqProducer = mqFactory.getProducer(MQTypeEnum.REDIS);
        deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        if (deadLetterService == null) {
            logger.info("死信队列服务未启用，跳过测试");
            return;
        }

        // 清理之前的测试数据
        List<DeadLetterMessage> oldMessages = deadLetterService.listDeadLetterMessages(0, 100);
        for (DeadLetterMessage message : oldMessages) {
            deadLetterService.deleteDeadLetterMessage(message.getId());
        }
    }

    /**
     * 测试批量发送消息并进入死信队列
     */
    @Test
    public void testBatchMessagesToDeadLetter() throws InterruptedException {
        if (deadLetterService == null) {
            return; // 死信队列服务未启用，跳过测试
        }

        // 设置测试消费者为失败模式
        batchTestConsumer.setShouldFail(true);
        batchTestConsumer.setFailureCount(10); // 设置失败次数大于最大重试次数，确保消息进入死信队列

        // 批量发送测试消息
        int batchSize = 10;
        List<String> messageIds = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            String messageId = UUID.randomUUID().toString().replace("-", "");
            messageIds.add(messageId);

            Map<String, String> userProperties = new HashMap<>();
            userProperties.put("messageId", messageId);
            userProperties.put("batchIndex", String.valueOf(i));

            String messageBody = "这是批量测试消息 " + (i + 1);
            Map<String, String> headers = new HashMap<>();
            headers.put("messageId", messageId);
            headers.put("batchIndex", String.valueOf(i));

            logger.info("发送批量测试消息 {}/{}, ID: {}", i + 1, batchSize, messageId);
            mqProducer.send("batch-test-topic", "batch-tag", messageBody, headers);
        }

        // 验证消息已进入死信队列
        // 由于重试间隔是2秒，最大重试2次，重试倍数1.5，总重试时间约为2+3=5秒
        // 加上处理时间和延迟，等待30秒确保所有重试完成
        logger.info("等待消息重试完成并进入死信队列...");
        
        // 分阶段检查死信队列，避免一次性等待太久
        List<DeadLetterMessage> messages = null;
        for (int i = 0; i < 6; i++) {
            Thread.sleep(5000); // 每5秒检查一次
            messages = deadLetterService.listDeadLetterMessages(0, batchSize * 2);
            logger.info("第{}次检查死信队列，当前消息数量: {}", i + 1, messages.size());
            if (messages.size() >= batchSize) {
                logger.info("所有消息已进入死信队列，提前结束等待");
                break;
            }
        }
        
        assertFalse(messages.isEmpty(), "死信队列中应该有消息");

        // 验证所有消息都进入了死信队列
        Set<String> deadLetterMessageIds = messages.stream()
            .map(message -> {
                if (message.getProperties() != null && message.getProperties().containsKey("messageId")) {
                    return (String) message.getProperties().get("messageId");
                }
                return message.getOriginalMessageId();
            })
            .collect(Collectors.toSet());

        for (String messageId : messageIds) {
            assertTrue(deadLetterMessageIds.contains(messageId), "消息ID " + messageId + " 应该在死信队列中");
        }

        logger.info("所有 {} 条消息都已进入死信队列", batchSize);
    }

    /**
     * 测试批量重新投递死信消息
     */
    @Test
    public void testBatchRedeliverDeadLetterMessages() throws InterruptedException {
        if (deadLetterService == null) {
            return; // 死信队列服务未启用，跳过测试
        }

        // 先批量发送消息并让它们失败进入死信队列
        testBatchMessagesToDeadLetter();

        // 获取死信队列中的所有消息
        List<DeadLetterMessage> deadLetterMessages = deadLetterService.listDeadLetterMessages(0, 20);
        assertFalse(deadLetterMessages.isEmpty(), "死信队列中应该有消息");

        // 收集所有死信消息的ID
        List<String> deadLetterIds = deadLetterMessages.stream()
            .map(DeadLetterMessage::getId)
            .collect(Collectors.toList());

        // 设置测试消费者为成功模式
        batchTestConsumer.setShouldFail(false);

        // 设置成功处理的倒计时锁
        CountDownLatch successLatch = new CountDownLatch(deadLetterIds.size());
        batchTestConsumer.setLatch(successLatch);

        // 批量重新投递死信消息
        logger.info("批量重新投递 {} 条死信消息", deadLetterIds.size());
        int successCount = deadLetterService.redeliverMessages(deadLetterIds);

        assertEquals(deadLetterIds.size(), successCount, "应该成功重新投递所有死信消息");

        // 等待所有消息成功处理
        boolean successAwait = successLatch.await(60, TimeUnit.SECONDS);
        assertTrue(successAwait, "消息重新处理超时");

        // 验证所有消息已从死信队列中删除
        Thread.sleep(1000); // 等待一段时间确保消息已从死信队列中删除

        // 检查死信队列中是否还有这些消息
        List<DeadLetterMessage> remainingMessages = deadLetterService.listDeadLetterMessages(0, 20);
        Set<String> remainingIds = remainingMessages.stream()
            .map(DeadLetterMessage::getId)
            .collect(Collectors.toSet());

        for (String id : deadLetterIds) {
            assertFalse(remainingIds.contains(id), "消息ID " + id + " 应该已从死信队列中删除");
        }

        logger.info("所有 {} 条消息都已成功重新投递并从死信队列中删除", deadLetterIds.size());
    }

    /**
     * 测试批量删除死信消息
     */
    @Test
    public void testBatchDeleteDeadLetterMessages() throws InterruptedException {
        if (deadLetterService == null) {
            return; // 死信队列服务未启用，跳过测试
        }

        // 先批量发送消息并让它们失败进入死信队列
        testBatchMessagesToDeadLetter();

        // 获取死信队列中的所有消息
        List<DeadLetterMessage> deadLetterMessages = deadLetterService.listDeadLetterMessages(0, 20);
        assertFalse(deadLetterMessages.isEmpty(), "死信队列中应该有消息");

        // 收集所有死信消息的ID
        List<String> deadLetterIds = deadLetterMessages.stream()
            .map(DeadLetterMessage::getId)
            .collect(Collectors.toList());

        // 批量删除死信消息
        logger.info("批量删除 {} 条死信消息", deadLetterIds.size());
        int deleteCount = deadLetterService.deleteDeadLetterMessages(deadLetterIds);

        assertEquals(deadLetterIds.size(), deleteCount, "应该成功删除所有死信消息");

        // 验证所有消息已从死信队列中删除
        Thread.sleep(1000); // 等待一段时间确保消息已从死信队列中删除

        // 检查死信队列中是否还有这些消息
        List<DeadLetterMessage> remainingMessages = deadLetterService.listDeadLetterMessages(0, 20);
        Set<String> remainingIds = remainingMessages.stream()
            .map(DeadLetterMessage::getId)
            .collect(Collectors.toSet());

        for (String id : deadLetterIds) {
            assertFalse(remainingIds.contains(id), "消息ID " + id + " 应该已从死信队列中删除");
        }

        logger.info("所有 {} 条消息都已成功从死信队列中删除", deadLetterIds.size());
    }

    /**
     * 批量测试消费者
     */
    @Component
    public static class BatchTestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(BatchTestConsumer.class);

        private boolean shouldFail = false;
        private int failureCount = 0;
        private Map<String, Integer> currentFailureCounts = new HashMap<>();
        private CountDownLatch latch;

        @MQConsumer(mqType = MQTypeEnum.REDIS, topic = "batch-test-topic", tag = "batch-tag", mode = MessageMode.UNICAST, consumerGroup = "user-service-group-dead")
        public void consumeMessage(String message) {
            logger.info("接收到批量测试消息: {}", message);

            if (shouldFail) {
                // 使用消息内容作为key来跟踪失败次数
                int currentFailureCount = currentFailureCounts.getOrDefault(message, 0);
                currentFailureCounts.put(message, currentFailureCount + 1);
                logger.error("消息处理失败，消息: {}, 当前失败次数: {}", message, currentFailureCount + 1);
                throw new RuntimeException("模拟消息处理失败");
            }

            // 消息处理成功
            logger.info("消息处理成功，消息: {}", message);

            // 只有成功处理时才通知测试线程
            if (latch != null) {
                latch.countDown();
            }

            // 重置失败计数
            currentFailureCounts.remove(message);
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
            this.currentFailureCounts.clear();
        }

        public void setFailureCount(int failureCount) {
            this.failureCount = failureCount;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }
}