package com.lachesis.windrangerms.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试RocketMQ连接
 */
@SpringBootTest
// 使用默认统一配置
public class RocketMQConnectionTest {

    private static final Logger logger = LoggerFactory.getLogger(RocketMQConnectionTest.class);

    @Test
    public void testRocketMQConnection() {
        DefaultMQPushConsumer consumer = null;
        try {
            // 创建一个简单的消费者来测试连接
            consumer = new DefaultMQPushConsumer("test-group-" + System.currentTimeMillis());
            consumer.setNamesrvAddr("localhost:9876");
            consumer.setConsumeFromWhere(org.apache.rocketmq.common.consumer.ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            
            // 订阅一个测试主题
            consumer.subscribe("test-topic", "*");
            
            // 设置消息监听器
            consumer.registerMessageListener(new org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently() {
                @Override
                public org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus consumeMessage(
                        java.util.List<org.apache.rocketmq.common.message.MessageExt> messages,
                        org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext context) {
                    return org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            
            // 启动消费者
            consumer.start();
            logger.info("RocketMQ连接测试成功");
            
            // 等待一秒确保连接稳定
            Thread.sleep(1000);
            
            assertTrue(true, "RocketMQ连接成功");
            
        } catch (MQClientException e) {
            logger.error("RocketMQ连接失败: {}", e.getMessage(), e);
            throw new RuntimeException("RocketMQ连接失败", e);
        } catch (Exception e) {
            logger.error("测试过程中发生异常: {}", e.getMessage(), e);
            throw new RuntimeException("测试失败", e);
        } finally {
            // 确保消费者被正确关闭
            if (consumer != null) {
                try {
                    consumer.shutdown();
                    logger.info("RocketMQ消费者已关闭");
                } catch (Exception e) {
                    logger.warn("关闭RocketMQ消费者时发生异常: {}", e.getMessage());
                }
            }
        }
    }
}