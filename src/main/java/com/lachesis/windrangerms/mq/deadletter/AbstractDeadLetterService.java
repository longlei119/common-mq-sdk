package com.lachesis.windrangerms.mq.deadletter;

import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterStatusEnum;
import com.lachesis.windrangerms.mq.delay.model.MessageStatusEnum;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

/**
 * 死信队列服务抽象实现
 * 提供通用的死信队列处理逻辑
 */
public abstract class AbstractDeadLetterService implements DeadLetterService {
    
    protected static final Logger log = LoggerFactory.getLogger(AbstractDeadLetterService.class);

    @Autowired
    protected MQConfig mqConfig;
    
    @Autowired
    protected MQFactory mqFactory;
    
    /**
     * 重新投递死信消息
     *
     * @param id 消息ID
     * @return 是否重新投递成功
     */
    @Override
    public boolean redeliverMessage(String id) {
        // 获取死信消息
        DeadLetterMessage message = getDeadLetterMessage(id);
        if (message == null) {
            log.warn("Dead letter message not found: {}", id);
            return false;
        }
        
        // 检查消息状态
        if (message.getStatusEnum() != DeadLetterStatusEnum.PENDING) {
            log.warn("Cannot redeliver message with status {}: {}", message.getStatusEnum(), id);
            return false;
        }
        
        try {
            // 获取对应的MQ生产者
            MQTypeEnum mqTypeEnum = MQTypeEnum.fromString(message.getMqType());
            if (mqTypeEnum == null) {
                log.error("Invalid MQ type: {}", message.getMqType());
                recordRetryHistory(id, false, "Invalid MQ type: " + message.getMqType());
                return false;
            }
            
            MQProducer producer = mqFactory.getProducer(mqTypeEnum);
            if (producer == null) {
                log.error("No producer found for MQ type: {}", message.getMqType());
                recordRetryHistory(id, false, "No producer found for MQ type: " + message.getMqType());
                return false;
            }
            
            // 更新消息状态为重新投递中
            message.setStatusEnum(DeadLetterStatusEnum.REDELIVERED);
            message.setRetryCount(message.getRetryCount() + 1);
            updateDeadLetterMessage(message);
            
            // 重新发送消息
            boolean result = producer.send(message.getTopic(), message.getTag(), message.getBody(), message.getProperties());
            
            // 记录重试结果
            if (result) {
                message.setStatusEnum(DeadLetterStatusEnum.DELIVERED);
                updateDeadLetterMessage(message);
                recordRetryHistory(id, true, null);
                log.info("Successfully redelivered dead letter message: {}", id);
            } else {
                message.setStatusEnum(DeadLetterStatusEnum.FAILED);
                updateDeadLetterMessage(message);
                recordRetryHistory(id, false, "Producer failed to send message");
                log.error("Failed to redeliver dead letter message: {}", id);
            }
            
            return result;
        } catch (Exception e) {
            log.error("Error redelivering dead letter message: {}", id, e);
            message.setStatusEnum(DeadLetterStatusEnum.FAILED);
            updateDeadLetterMessage(message);
            recordRetryHistory(id, false, e.getMessage());
            return false;
        }
    }
    
    /**
     * 批量重新投递死信消息
     *
     * @param ids 消息ID列表
     * @return 成功重新投递的消息数量
     */
    @Override
    public int redeliverMessages(List<String> ids) {
        int successCount = 0;
        for (String id : ids) {
            if (redeliverMessage(id)) {
                successCount++;
            }
        }
        return successCount;
    }
    
    /**
     * 计算重试延迟时间
     *
     * @param retryCount 当前重试次数
     * @return 延迟时间（毫秒）
     */
    protected long calculateRetryDelay(int retryCount) {
        MQConfig.DeadLetterProperties.RetryConfig retryConfig = mqConfig.getDeadLetter().getRetry();
        long delay = retryConfig.getRetryInterval();
        
        // 使用指数退避策略计算延迟时间
        for (int i = 1; i < retryCount && i < retryConfig.getMaxRetries(); i++) {
            delay *= retryConfig.getRetryMultiplier();
        }
        
        return delay;
    }
    
    /**
     * 生成唯一ID
     *
     * @return 唯一ID
     */
    protected String generateId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 更新死信消息
     *
     * @param message 死信消息
     * @return 是否更新成功
     */
    protected abstract boolean updateDeadLetterMessage(DeadLetterMessage message);
}