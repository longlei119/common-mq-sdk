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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ActiveMQ死信队列测试
 * 测试ActiveMQ消息消费失败后进入死信队列的功能
 */
@Slf4j
@SpringBootTest
public class ActiveMQDeadLetterTest {

    @Autowired
    private MQFactory mqFactory;

    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;

    @Autowired
    private MQConfig mqConfig;

    @Autowired
    private ActiveMQTestConsumer activeMQTestConsumer;

    @Autowired
    private JmsTemplate jmsTemplate;

    private MQProducer mqProducer;

    @Autowired
    private org.springframework.context.ApplicationContext applicationContext;

    @BeforeEach
    public void setup() {
        // 检查ActiveMQ是否启用
        if (mqConfig.getActivemq() == null) {
            log.info("ActiveMQ未配置，跳过测试");
            return;
        }

        // 调试：检查Spring容器中的Bean
        log.info("=== Spring容器Bean检查 ===");
        try {
            org.springframework.jms.core.JmsTemplate jmsTemplateBean = applicationContext.getBean(org.springframework.jms.core.JmsTemplate.class);
            log.info("✓ JmsTemplate Bean存在: {}", jmsTemplateBean.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("✗ JmsTemplate Bean不存在: {}", e.getMessage());
        }

        try {
            com.lachesis.windrangerms.mq.producer.impl.ActiveMQProducer activeMQProducerBean =
                applicationContext.getBean(com.lachesis.windrangerms.mq.producer.impl.ActiveMQProducer.class);
            log.info("✓ ActiveMQProducer Bean存在: {}", activeMQProducerBean.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("✗ ActiveMQProducer Bean不存在: {}", e.getMessage());
        }

        try {
            com.lachesis.windrangerms.mq.consumer.impl.ActiveMQConsumer activeMQConsumerBean =
                applicationContext.getBean(com.lachesis.windrangerms.mq.consumer.impl.ActiveMQConsumer.class);
            log.info("✓ ActiveMQConsumer Bean存在: {}", activeMQConsumerBean.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("✗ ActiveMQConsumer Bean不存在: {}", e.getMessage());
        }

        try {
            com.lachesis.windrangerms.mq.consumer.MQConsumerManager mqConsumerManager =
                applicationContext.getBean(com.lachesis.windrangerms.mq.consumer.MQConsumerManager.class);
            log.info("✓ MQConsumerManager Bean存在: {}", mqConsumerManager.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("✗ MQConsumerManager Bean不存在: {}", e.getMessage());
        }

        try {
            com.lachesis.windrangerms.mq.processor.MQConsumerAnnotationProcessor processor =
                applicationContext.getBean(com.lachesis.windrangerms.mq.processor.MQConsumerAnnotationProcessor.class);
            log.info("✓ MQConsumerAnnotationProcessor Bean存在: {}", processor.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("✗ MQConsumerAnnotationProcessor Bean不存在: {}", e.getMessage());
        }

        log.info("=== MQFactory状态检查 ===");
        log.info("MQFactory中注册的生产者: {}", mqFactory.getClass().getSimpleName());

        try {
            mqProducer = mqFactory.getProducer(MQTypeEnum.ACTIVE_MQ);
            log.info("✓ 成功从MQFactory获取ActiveMQ生产者");
        } catch (Exception e) {
            log.error("✗ 从MQFactory获取ActiveMQ生产者失败: {}", e.getMessage());
            throw e;
        }

        log.info("ActiveMQ死信队列测试初始化完成");
    }

    /**
     * 测试ActiveMQ死信队列基本功能
     */
    @Test
    public void testActiveMQDeadLetter() throws InterruptedException {
        // 检查ActiveMQ是否启用
        if (mqConfig.getActivemq() == null) {
            log.info("ActiveMQ未配置，跳过测试");
            return;
        }

        log.info("开始ActiveMQ死信测试");

        // 添加调试日志：检查消费者管理器状态
        try {
            log.info("检查消费者注册状态...");
            log.info("MQFactory类型: {}", mqFactory.getClass().getSimpleName());
            log.info("ActiveMQTestConsumer实例: {}", activeMQTestConsumer != null ? "已创建" : "未创建");
            log.info("JmsTemplate实例: {}", jmsTemplate != null ? "已创建" : "未创建");
            log.info("JmsTemplate配置: pubSubDomain={}", jmsTemplate.isPubSubDomain());
        } catch (Exception e) {
            log.error("获取消费者信息失败", e);
        }

        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信服务不应为空");

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);
        headers.put("testType", "deadLetter");

        CountDownLatch latch = new CountDownLatch(1);
        activeMQTestConsumer.setLatch(latch);
        log.info("设置CountDownLatch完成，等待消息数量: {}", latch.getCount());

        // 等待ActiveMQ消费者完全准备好
        Thread.sleep(3000);
        log.info("ActiveMQ消费者准备完成，开始发送测试消息");

        // 添加调试：检查队列是否存在消息（使用非阻塞方式）
        try {
            String destination = "test-dead-letter-topic-activemq.test-tag";
            // 使用带超时的receiveAndConvert，避免无限阻塞
            jmsTemplate.setReceiveTimeout(1000); // 设置1秒超时
            Object existingMessage = jmsTemplate.receiveAndConvert(destination);
            if (existingMessage != null) {
                log.info("队列中发现现有消息: {}", existingMessage);
            } else {
                log.info("队列中无现有消息");
            }
        } catch (Exception e) {
            log.warn("检查队列现有消息失败: {}", e.getMessage());
        }

        // 使用ActiveMQ生产者发送消息
        log.info("发送ActiveMQ测试消息，ID: {}", messageId);
        mqProducer.send("test-dead-letter-topic-activemq", "test-tag", "这是一条ActiveMQ死信测试消息", headers);

        // 等待消息被消费并进入死信队列
        log.info("开始等待消息处理，超时时间: 30秒");
        boolean await = latch.await(30, TimeUnit.SECONDS);
        log.info("等待结果: {}, 剩余计数: {}", await, latch.getCount());

        if (!await) {
            // 如果消息没有被消费，尝试同步接收检查
            log.warn("消息未被消费，尝试同步接收检查...");
            try {
                String destination = "test-dead-letter-topic-activemq.test-tag";
                // 设置超时避免阻塞
                jmsTemplate.setReceiveTimeout(2000); // 设置2秒超时
                Object syncMessage = jmsTemplate.receiveAndConvert(destination);
                if (syncMessage != null) {
                    log.warn("队列中存在消息但消费者未收到: {}", syncMessage);
                } else {
                    log.warn("队列中无消息");
                }
            } catch (Exception e) {
                log.error("同步接收检查失败: {}", e.getMessage());
            }
        }

        assertTrue(await, "消息未被消费或未进入死信队列");

        // 验证消息已进入死信队列
        Thread.sleep(2000); // 等待一段时间确保消息已进入死信队列

        List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
        log.info("死信队列中的消息数量: {}", messages.size());
        for (DeadLetterMessage msg : messages) {
            log.info("死信消息: ID={}, Topic={}, Tag={}, MQType={}, Properties={}",
                msg.getOriginalMessageId(), msg.getOriginalTopic(), msg.getOriginalTag(),
                msg.getMqType(), msg.getProperties());
        }

        boolean found = false;
        for (DeadLetterMessage message : messages) {
            // 检查多种匹配条件：原始消息ID、属性中的messageId、或者是ActiveMQ相关的消息
            boolean isMatch = message.getOriginalMessageId().equals(messageId) ||
                (message.getProperties() != null && messageId.equals(message.getProperties().get("messageId"))) ||
                ("ACTIVE_MQ".equals(message.getMqType()) &&
                    "test-dead-letter-topic-activemq".equals(message.getOriginalTopic()) &&
                    "test-tag".equals(message.getOriginalTag()) &&
                    message.getOriginalBody() != null &&
                    message.getOriginalBody().contains("这是一条ActiveMQ死信测试消息"));

            if (isMatch) {
                found = true;
                assertEquals("ACTIVE_MQ", message.getMqType(), "MQ类型应该是ACTIVE_MQ");
                assertEquals("test-dead-letter-topic-activemq", message.getOriginalTopic(), "原始Topic不匹配");
                assertEquals("test-tag", message.getOriginalTag(), "原始Tag不匹配");
                assertTrue(message.getOriginalBody().contains("这是一条ActiveMQ死信测试消息"), "原始Body应包含测试消息");
                assertNotNull(message.getDeadLetterTime(), "死信时间不应为空");
                assertNotNull(message.getFailureReason(), "失败原因不应为空");
                log.info("实际失败原因: [{}]", message.getFailureReason());
                assertTrue(message.getFailureReason().contains("执行MQ消费者方法失败") || message.getFailureReason().contains("模拟消费失败"),
                    "失败原因应包含失败信息，实际内容: " + message.getFailureReason());

                if (message.getRetryHistory() != null && !message.getRetryHistory().isEmpty()) {
                    RetryHistory lastRetry = message.getRetryHistory().get(message.getRetryHistory().size() - 1);
                    assertNotNull(lastRetry.getFailureReason(), "错误信息不应为空");
                    assertTrue(lastRetry.getFailureReason().contains("模拟消费失败"), "错误信息应包含失败原因");
                }

                log.info("在死信队列中找到ActiveMQ消息: {}", JSON.toJSONString(message));
                break;
            }
        }

        assertTrue(found, "应该在死信队列中找到ActiveMQ消息");
    }

    /**
     * ActiveMQ测试消费者
     * 模拟消费失败场景
     */
    @Component
    public static class ActiveMQTestConsumer {

        private CountDownLatch latch;
        private int retryCount = 0;

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public void setRetryCount(int retryCount) {
            this.retryCount = retryCount;
        }

        @MQConsumer(topic = "test-dead-letter-topic-activemq", tag = "test-tag", mqType = MQTypeEnum.ACTIVE_MQ)
        public void consumeDeadLetterMessage(String message) {
            log.info("=== ActiveMQ消费者被调用 ===");
            log.info("收到死信测试消息: {}", message);
            log.info("当前latch状态: {}", latch != null ? "已设置，计数=" + latch.getCount() : "未设置");

            // 模拟消费失败
            if (latch != null) {
                log.info("执行latch.countDown()，当前计数: {}", latch.getCount());
                latch.countDown();
                log.info("latch.countDown()执行完成，新计数: {}", latch.getCount());
            }
            log.info("准备抛出异常模拟消费失败");
            throw new RuntimeException("模拟消费失败 - ActiveMQ死信测试");
        }
    }
}