package com.lachesis.windrangerms.mq.example;

import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.enums.MessageAckResult;
import com.lachesis.windrangerms.mq.enums.MessageMode;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * MQ消费者示例类
 * 展示如何使用新的MessageAckResult返回值以及向后兼容的void方法
 */
@Slf4j
@Component
public class ExampleMQConsumer {

    /**
     * 使用新的MessageAckResult返回值的消费者方法
     * 可以精确控制消息的ACK行为
     */
    @MQConsumer(
        mqType = MQTypeEnum.RABBIT_MQ,
        topic = "example.topic",
        tag = "example.tag",
        mode = MessageMode.UNICAST,
        consumerGroup = "example-group"
    )
    public MessageAckResult handleMessageWithAckResult(String message) {
        try {
            log.info("处理消息: {}", message);
            
            // 模拟业务逻辑
            if (message.contains("success")) {
                // 业务处理成功
                log.info("消息处理成功");
                return MessageAckResult.SUCCESS;
            } else if (message.contains("retry")) {
                // 需要重试
                log.warn("消息处理失败，需要重试");
                return MessageAckResult.RETRY;
            } else if (message.contains("reject")) {
                // 拒绝消息，直接进入死信队列
                log.error("消息被拒绝，直接进入死信队列");
                return MessageAckResult.REJECT;
            } else if (message.contains("ignore")) {
                // 忽略消息
                log.info("消息被忽略");
                return MessageAckResult.IGNORE;
            } else {
                // 默认成功
                return MessageAckResult.SUCCESS;
            }
        } catch (Exception e) {
            log.error("处理消息时发生异常", e);
            // 异常时返回RETRY，让系统进行重试
            return MessageAckResult.RETRY;
        }
    }

    /**
     * 传统的void方法消费者（向后兼容）
     * 通过异常来控制ACK行为
     */
    @MQConsumer(
        mqType = MQTypeEnum.RABBIT_MQ,
        topic = "legacy.topic",
        tag = "legacy.tag",
        mode = MessageMode.UNICAST,
        consumerGroup = "legacy-group"
    )
    public void handleLegacyMessage(String message) {
        try {
            log.info("处理传统消息: {}", message);
            
            // 模拟业务逻辑
            if (message.contains("error")) {
                // 抛出异常触发重试
                throw new RuntimeException("业务处理失败");
            }
            
            // 正常执行完成，自动ACK
            log.info("传统消息处理成功");
        } catch (Exception e) {
            log.error("处理传统消息时发生异常", e);
            // 重新抛出异常，触发重试机制
            throw e;
        }
    }

    /**
     * 复杂业务逻辑的消费者示例
     * 展示如何根据不同的业务场景返回不同的ACK结果
     */
    @MQConsumer(
        mqType = MQTypeEnum.REDIS,
        topic = "complex.topic",
        tag = "complex.tag",
        mode = MessageMode.BROADCAST
    )
    public MessageAckResult handleComplexMessage(String message) {
        log.info("处理复杂消息: {}", message);
        
        try {
            // 解析消息
            if (message == null || message.trim().isEmpty()) {
                log.warn("收到空消息，忽略处理");
                return MessageAckResult.IGNORE;
            }
            
            // 模拟数据库操作
            if (message.contains("db_error")) {
                log.error("数据库操作失败，需要重试");
                return MessageAckResult.RETRY;
            }
            
            // 模拟业务规则验证
            if (message.contains("invalid_data")) {
                log.error("数据格式无效，拒绝处理");
                return MessageAckResult.REJECT;
            }
            
            // 模拟外部服务调用
            if (message.contains("service_unavailable")) {
                log.warn("外部服务不可用，稍后重试");
                return MessageAckResult.RETRY;
            }
            
            // 正常业务处理
            log.info("复杂消息处理成功");
            return MessageAckResult.SUCCESS;
            
        } catch (Exception e) {
            log.error("处理复杂消息时发生未预期异常", e);
            // 对于未预期的异常，选择重试
            return MessageAckResult.RETRY;
        }
    }
}