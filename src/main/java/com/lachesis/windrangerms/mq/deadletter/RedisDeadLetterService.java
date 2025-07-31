package com.lachesis.windrangerms.mq.deadletter;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterStatusEnum;
import com.lachesis.windrangerms.mq.deadletter.model.RetryHistory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Redis实现的死信队列服务
 */
@Slf4j
public class RedisDeadLetterService extends AbstractDeadLetterService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String RETRY_HISTORY_KEY_PREFIX = "dead_letter:retry_history:";

    /**
     * 将消息保存到死信队列
     *
     * @param message 死信消息
     * @return 是否保存成功
     */
    @Override
    public boolean saveDeadLetterMessage(DeadLetterMessage message) {
        try {
            if (message.getId() == null) {
                message.setId(generateId());
            }
            
            if (message.getCreateTimestamp() <= 0) {
                message.setCreateTimestamp(System.currentTimeMillis());
            }
            
            if (message.getStatusEnum() == null) {
                message.setStatusEnum(DeadLetterStatusEnum.PENDING);
            }
            
            String messageKey = getMessageKey(message.getId());
            String messageJson = JSON.toJSONString(message);
            
            // 保存消息详情
            redisTemplate.opsForValue().set(messageKey, messageJson);
            
            // 设置过期时间
            long expireTime = mqConfig.getDeadLetter().getRedis().getMessageExpireTime();
            redisTemplate.expire(messageKey, expireTime, TimeUnit.MILLISECONDS);
            
            // 将消息ID添加到队列
            redisTemplate.opsForList().rightPush(mqConfig.getDeadLetter().getRedis().getQueueKey(), message.getId());
            
            log.info("Saved dead letter message: {}", message.getId());
            return true;
        } catch (Exception e) {
            log.error("Failed to save dead letter message", e);
            return false;
        }
    }

    /**
     * 获取死信队列中的消息列表
     *
     * @param offset 偏移量
     * @param limit  限制数量
     * @return 死信消息列表
     */
    @Override
    public List<DeadLetterMessage> listDeadLetterMessages(int offset, int limit) {
        try {
            // 获取队列总长度
            Long totalSize = redisTemplate.opsForList().size(mqConfig.getDeadLetter().getRedis().getQueueKey());
            if (totalSize == null || totalSize == 0) {
                return Collections.emptyList();
            }
            
            // 计算从右端开始的索引范围，获取最新的消息
            long startIndex = Math.max(0, totalSize - offset - limit);
            long endIndex = totalSize - offset - 1;
            
            if (startIndex > endIndex) {
                return Collections.emptyList();
            }
            
            // 获取队列中的消息ID列表（从最新到最旧）
            List<String> messageIds = redisTemplate.opsForList().range(
                    mqConfig.getDeadLetter().getRedis().getQueueKey(), startIndex, endIndex);
            
            if (messageIds == null || messageIds.isEmpty()) {
                return Collections.emptyList();
            }
            
            // 反转列表，使最新的消息排在前面
            Collections.reverse(messageIds);
            
            // 批量获取消息详情
            List<DeadLetterMessage> messages = new ArrayList<>();
            for (String id : messageIds) {
                DeadLetterMessage message = getDeadLetterMessage(id);
                if (message != null) {
                    messages.add(message);
                }
            }
            
            return messages;
        } catch (Exception e) {
            log.error("Failed to list dead letter messages", e);
            return Collections.emptyList();
        }
    }

    /**
     * 获取死信队列中的消息总数
     *
     * @return 消息总数
     */
    @Override
    public long countDeadLetterMessages() {
        try {
            Long size = redisTemplate.opsForList().size(mqConfig.getDeadLetter().getRedis().getQueueKey());
            return size != null ? size : 0;
        } catch (Exception e) {
            log.error("Failed to count dead letter messages", e);
            return 0;
        }
    }

    /**
     * 根据ID获取死信消息
     *
     * @param id 消息ID
     * @return 死信消息
     */
    @Override
    public DeadLetterMessage getDeadLetterMessage(String id) {
        try {
            String messageJson = redisTemplate.opsForValue().get(getMessageKey(id));
            if (!StringUtils.hasText(messageJson)) {
                return null;
            }
            
            return JSON.parseObject(messageJson, DeadLetterMessage.class);
        } catch (Exception e) {
            log.error("Failed to get dead letter message: {}", id, e);
            return null;
        }
    }

    /**
     * 删除死信消息
     *
     * @param id 消息ID
     * @return 是否删除成功
     */
    @Override
    public boolean deleteDeadLetterMessage(String id) {
        try {
            // 删除消息详情
            redisTemplate.delete(getMessageKey(id));
            
            // 从队列中删除消息ID
            redisTemplate.opsForList().remove(mqConfig.getDeadLetter().getRedis().getQueueKey(), 0, id);
            
            // 删除重试历史
            redisTemplate.delete(getRetryHistoryKey(id));
            
            log.info("Deleted dead letter message: {}", id);
            return true;
        } catch (Exception e) {
            log.error("Failed to delete dead letter message: {}", id, e);
            return false;
        }
    }

    /**
     * 批量删除死信消息
     *
     * @param ids 消息ID列表
     * @return 成功删除的消息数量
     */
    @Override
    public int deleteDeadLetterMessages(List<String> ids) {
        int successCount = 0;
        for (String id : ids) {
            if (deleteDeadLetterMessage(id)) {
                successCount++;
            }
        }
        return successCount;
    }

    /**
     * 清理过期的死信消息
     * 定时任务，根据配置的清理周期执行
     */
    @Scheduled(fixedDelayString = "${mq.dead-letter.cleanup.cleanup-interval:86400000}")
    public void scheduledCleanup() {
        if (!mqConfig.getDeadLetter().getCleanup().isEnabled()) {
            return;
        }
        
        long expireTime = System.currentTimeMillis() - mqConfig.getDeadLetter().getCleanup().getMessageRetentionTime();
        int count = cleanupExpiredMessages(expireTime);
        log.info("Scheduled cleanup: removed {} expired dead letter messages", count);
    }

    /**
     * 清理过期消息
     *
     * @return 清理的消息数量
     */
    @Override
    public int cleanupExpiredMessages() {
        long expireTime = System.currentTimeMillis() - mqConfig.getDeadLetter().getCleanup().getMessageRetentionTime();
        return cleanupExpiredMessages(expireTime);
    }

    /**
     * 清理过期的死信消息
     *
     * @param expireTime 过期时间（毫秒）
     * @return 清理的消息数量
     */
    public int cleanupExpiredMessages(long expireTime) {
        try {
            // 获取所有消息ID
            List<String> allMessageIds = redisTemplate.opsForList().range(
                    mqConfig.getDeadLetter().getRedis().getQueueKey(), 0, -1);
            
            if (allMessageIds == null || allMessageIds.isEmpty()) {
                return 0;
            }
            
            List<String> expiredIds = new ArrayList<>();
            
            // 检查每个消息是否过期
            for (String id : allMessageIds) {
                DeadLetterMessage message = getDeadLetterMessage(id);
                if (message != null && message.getCreateTimestamp() < expireTime) {
                    expiredIds.add(id);
                }
            }
            
            // 删除过期消息
            return deleteDeadLetterMessages(expiredIds);
        } catch (Exception e) {
            log.error("Failed to cleanup expired messages", e);
            return 0;
        }
    }

    /**
     * 记录重试历史
     *
     * @param messageId 消息ID
     * @param result    重试结果
     * @param reason    失败原因（如果失败）
     * @return 是否记录成功
     */
    @Override
    public boolean recordRetryHistory(String messageId, boolean result, String reason) {
        try {
            DeadLetterMessage message = getDeadLetterMessage(messageId);
            if (message == null) {
                log.warn("Cannot record retry history for non-existent message: {}", messageId);
                return false;
            }
            
            RetryHistory history = RetryHistory.builder()
                    .id(generateId())
                    .messageId(messageId)
                    .retryTimestamp(System.currentTimeMillis())
                    .success(result)
                    .failureReason(reason)
                    .retryCount(message.getRetryCount())
                    .build();
            
            String historyJson = JSON.toJSONString(history);
            String historyKey = getRetryHistoryKey(messageId);
            
            // 将重试历史添加到列表
            redisTemplate.opsForList().rightPush(historyKey, historyJson);
            
            // 设置过期时间与消息一致
            redisTemplate.expire(historyKey, mqConfig.getDeadLetter().getRedis().getMessageExpireTime(), TimeUnit.MILLISECONDS);
            
            return true;
        } catch (Exception e) {
            log.error("Failed to record retry history for message: {}", messageId, e);
            return false;
        }
    }

    /**
     * 获取消息的重试历史
     *
     * @param messageId 消息ID
     * @return 重试历史列表
     */
    @Override
    public List<RetryHistory> getRetryHistory(String messageId) {
        try {
            String historyKey = getRetryHistoryKey(messageId);
            List<String> historyJsonList = redisTemplate.opsForList().range(historyKey, 0, -1);
            
            if (historyJsonList == null || historyJsonList.isEmpty()) {
                return Collections.emptyList();
            }
            
            return historyJsonList.stream()
                    .map(json -> JSON.parseObject(json, RetryHistory.class))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to get retry history for message: {}", messageId, e);
            return Collections.emptyList();
        }
    }

    /**
     * 更新死信消息
     *
     * @param message 死信消息
     * @return 是否更新成功
     */
    @Override
    protected boolean updateDeadLetterMessage(DeadLetterMessage message) {
        try {
            String messageKey = getMessageKey(message.getId());
            String messageJson = JSON.toJSONString(message);
            
            // 更新消息详情
            redisTemplate.opsForValue().set(messageKey, messageJson);
            
            // 重置过期时间
            redisTemplate.expire(messageKey, mqConfig.getDeadLetter().getRedis().getMessageExpireTime(), TimeUnit.MILLISECONDS);
            
            return true;
        } catch (Exception e) {
            log.error("Failed to update dead letter message: {}", message.getId(), e);
            return false;
        }
    }

    /**
     * 获取消息键名
     *
     * @param id 消息ID
     * @return Redis键名
     */
    private String getMessageKey(String id) {
        return mqConfig.getDeadLetter().getRedis().getKeyPrefix() + "message:" + id;
    }

    /**
     * 获取重试历史键名
     *
     * @param messageId 消息ID
     * @return Redis键名
     */
    private String getRetryHistoryKey(String messageId) {
        return RETRY_HISTORY_KEY_PREFIX + messageId;
    }
}