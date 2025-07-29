package com.lachesis.windrangerms.mq.example;

import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.model.RetryHistory;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * 死信队列使用示例
 * 
 * 此示例展示了如何：
 * 1. 创建一个可能失败的消费者
 * 2. 查询和管理死信队列中的消息
 * 3. 重新投递死信消息
 */
@Component
public class DeadLetterQueueExample implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueExample.class);

    @Autowired
    private MQFactory mqFactory;

    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;

    @Override
    public void run(String... args) {
        // 检查死信队列服务是否可用
        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        if (deadLetterService == null) {
            logger.info("死信队列服务未启用，请检查配置");
            return;
        }

        // 发送一条可能会失败的消息
        String messageId = "test-message-" + System.currentTimeMillis();
        Map<String, String> properties = new HashMap<>();
        properties.put("messageId", messageId);
        
        logger.info("发送测试消息，ID: {}", messageId);
        MQProducer mqProducer = mqFactory.getProducer(MQTypeEnum.KAFKA);
        mqProducer.send("test-topic", "test-tag", "这是一条测试消息，可能会失败", properties);

        // 等待消息处理（在实际应用中，这里应该有更好的方式来等待或检查）
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 查询死信队列中的消息
        logger.info("查询死信队列中的消息");
        List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
        logger.info("死信队列中有 {} 条消息", messages.size());

        // 打印消息详情
        for (DeadLetterMessage message : messages) {
            logger.info("死信消息: ID={}, 主题={}, 标签={}, 状态={}, 失败原因={}", 
                    message.getId(), message.getTopic(), message.getTag(), 
                    message.getStatusEnum(), message.getFailureReason());

            // 获取重试历史
            List<RetryHistory> retryHistories = deadLetterService.getRetryHistory(message.getId());
            logger.info("消息 {} 有 {} 条重试历史", message.getId(), retryHistories.size());
            for (RetryHistory history : retryHistories) {
                logger.info("重试历史: 时间={}, 成功={}, 重试次数={}", 
                        history.getRetryTimestamp(), history.isSuccess(), history.getRetryCount());
            }

            // 重新投递消息
            logger.info("尝试重新投递消息 {}", message.getId());
            boolean result = deadLetterService.redeliverMessage(message.getId());
            logger.info("重新投递结果: {}", result);
        }
    }

    /**
     * 测试消费者，有50%的概率会失败
     */
    @Component
    public static class TestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(TestConsumer.class);

        @MQConsumer(mqType = MQTypeEnum.KAFKA, topic = "test-topic", tag = "test-tag")
        public void consumeMessage(String message) {
            logger.info("接收到消息: {}", message);

            // 模拟随机失败
            if (Math.random() < 0.5) {
                logger.error("消息处理失败，抛出异常");
                throw new RuntimeException("模拟消息处理失败");
            }

            logger.info("消息处理成功");
        }
    }

    /**
     * 死信队列管理示例
     */
    @Component
    public static class DeadLetterQueueManager {

        private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueManager.class);

        @Autowired
        private DeadLetterServiceFactory deadLetterServiceFactory;

        /**
         * 清理过期的死信消息
         */
        public void cleanupExpiredMessages() {
            DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
            if (deadLetterService == null) {
                return;
            }

            logger.info("手动清理过期消息");
            deadLetterService.cleanupExpiredMessages();
        }

        /**
         * 删除指定的死信消息
         */
        public void deleteMessage(String messageId) {
            DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
            if (deadLetterService == null) {
                return;
            }

            logger.info("删除消息 {}", messageId);
            boolean result = deadLetterService.deleteDeadLetterMessage(messageId);
            logger.info("删除结果: {}", result);
        }

        /**
         * 重新投递所有死信消息
         */
        public void redeliverAllMessages() {
            DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
            if (deadLetterService == null) {
                return;
            }

            logger.info("重新投递所有死信消息");
            List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 100);
            for (DeadLetterMessage message : messages) {
                logger.info("重新投递消息 {}", message.getId());
                deadLetterService.redeliverMessage(message.getId());
            }
        }
    }
}