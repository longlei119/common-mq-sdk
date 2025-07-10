package com.example.mq.delay;

import com.alibaba.fastjson.JSON;
import com.example.mq.config.MQConfig;
import com.example.mq.delay.adapter.MQAdapter;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.enums.MQTypeEnum;
import com.example.mq.delay.model.MessageStatusEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 延迟消息发送器
 * 负责延迟消息的发送、扫描和投递
 */
@Slf4j
public class DelayMessageSender {

    private final StringRedisTemplate redisTemplate;
    private final Map<String, MQAdapter> mqAdapterMap;
    private final MQConfig mqConfig;
    
    /**
     * 消息重试最大次数
     */
    private static final int MAX_RETRY_COUNT = 3;
    
    /**
     * 消息清理阈值（毫秒），默认7天
     */
    private static final long MESSAGE_CLEANUP_THRESHOLD = 7 * 24 * 60 * 60 * 1000L;

    public DelayMessageSender(StringRedisTemplate redisTemplate, List<MQAdapter> mqAdapters, MQConfig mqConfig) {
        this.redisTemplate = redisTemplate;
        this.mqConfig = mqConfig;
        
        // 初始化MQ适配器映射
        this.mqAdapterMap = new ConcurrentHashMap<>();
        if (mqAdapters != null) {
            for (MQAdapter adapter : mqAdapters) {
                this.mqAdapterMap.put(adapter.getMQType(), adapter);
            }
        }
        
        log.info("延迟消息发送器初始化成功，已加载{}个MQ适配器", mqAdapterMap.size());
    }

    /**
     * 发送延迟消息
     *
     * @param mqType       MQ类型
     * @param topic        主题
     * @param tag          标签
     * @param body         消息体
     * @param properties   消息属性
     * @param delayMillis  延迟时间（毫秒）
     * @return 消息ID
     */
    public String sendDelayMessage(String topic, String tag, String body, 
                                 String mqType, long delayMillis) {
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId(UUID.randomUUID().toString());
        message.setTopic(topic);
        message.setTag(tag);
        message.setBody(body);
        message.setMqTypeEnum(MQTypeEnum.fromString(mqType));
        message.setDeliverTimestamp(System.currentTimeMillis() + delayMillis);
        
        return sendDelayMessage(message);
    }

    /**
     * 发送延迟消息
     *
     * @param message 延迟消息
     * @return 消息ID
     */
    public String sendDelayMessage(DelayMessage message) {
        try {
            // 将消息保存到Redis
            String messageKey = mqConfig.getDelay().getRedisKeyPrefix() + message.getId();
            redisTemplate.opsForValue().set(messageKey, JSON.toJSONString(message));
            
            // 将消息ID添加到有序集合，以投递时间为分数
            redisTemplate.opsForZSet().add(
                    mqConfig.getDelay().getRedisKeyPrefix() + "queue",
                    message.getId(),
                    message.getDeliverTimestamp()
            );
            
            log.info("延迟消息已保存: messageId={}, topic={}, tag={}, mqType={}, deliverTime={}", 
                    message.getId(), message.getTopic(), message.getTag(), 
                    message.getMqTypeEnum().getType(), Instant.ofEpochMilli(message.getDeliverTimestamp()));
            
            return message.getId();
        } catch (Exception e) {
            log.error("保存延迟消息失败: {}", e.getMessage(), e);
            throw new RuntimeException("保存延迟消息失败", e);
        }
    }

    /**
     * 定时扫描并投递到期的延迟消息
     */
    @Scheduled(fixedDelayString = "${mq.delay.scan-interval:1000}")
    public void scanAndDeliverMessages() {
        long now = System.currentTimeMillis();
        String queueKey = mqConfig.getDelay().getRedisKeyPrefix() + "queue";
        
        try {
            // 获取到期的消息ID
            Set<String> expiredMessageIds = redisTemplate.opsForZSet().rangeByScore(
                    queueKey, 0, now);
            
            if (expiredMessageIds == null || expiredMessageIds.isEmpty()) {
                return;
            }
            
            log.info("扫描到{}条到期的延迟消息", expiredMessageIds.size());
            
            // 处理到期的消息
            for (String messageId : expiredMessageIds) {
                processExpiredMessage(messageId);
            }
        } catch (Exception e) {
            log.error("扫描延迟消息异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 处理到期的延迟消息
     *
     * @param messageId 消息ID
     */
    private void processExpiredMessage(String messageId) {
        String messageKey = mqConfig.getDelay().getRedisKeyPrefix() + messageId;
        String queueKey = mqConfig.getDelay().getRedisKeyPrefix() + "queue";
        
        try {
            // 从Redis获取消息详情
            String messageJson = redisTemplate.opsForValue().get(messageKey);
            if (messageJson == null) {
                // 消息不存在，从队列中移除
                redisTemplate.opsForZSet().remove(queueKey, messageId);
                return;
            }
            
            // 解析消息
            DelayMessage message = JSON.parseObject(messageJson, DelayMessage.class);
            
            // 获取对应的MQ适配器
            MQAdapter adapter = mqAdapterMap.get(message.getMqTypeEnum().getType());
            if (adapter == null) {
                log.error("未找到对应的MQ适配器: messageId={}, mqType={}", 
                        messageId, message.getMqTypeEnum().getType());
                handleMessageFailure(message, "未找到对应的MQ适配器");
                return;
            }
            
            // 发送消息到目标MQ
            boolean success = adapter.send(message);
            
            if (success) {
                // 发送成功，更新消息状态并从队列中移除
                message.setStatusEnum(MessageStatusEnum.DELIVERED);
                redisTemplate.opsForValue().set(messageKey, JSON.toJSONString(message));
                redisTemplate.opsForZSet().remove(queueKey, messageId);
                
                log.info("延迟消息投递成功: messageId={}, topic={}, tag={}, mqType={}", 
                        messageId, message.getTopic(), message.getTag(), message.getMqTypeEnum().getType());
            } else {
                // 发送失败，处理失败逻辑
                handleMessageFailure(message, "MQ发送失败");
            }
        } catch (Exception e) {
            log.error("处理延迟消息异常: messageId={}, error={}", messageId, e.getMessage(), e);
            
            try {
                // 获取消息并处理失败
                String messageJson = redisTemplate.opsForValue().get(messageKey);
                if (messageJson != null) {
                    DelayMessage message = JSON.parseObject(messageJson, DelayMessage.class);
                    handleMessageFailure(message, e.getMessage());
                }
            } catch (Exception ex) {
                log.error("处理消息失败异常: messageId={}, error={}", messageId, ex.getMessage());
            }
        }
    }

    /**
     * 处理消息发送失败的情况
     *
     * @param message 延迟消息
     * @param reason  失败原因
     */
    private void handleMessageFailure(DelayMessage message, String reason) {
        String messageKey = mqConfig.getDelay().getRedisKeyPrefix() + message.getId();
        String queueKey = mqConfig.getDelay().getRedisKeyPrefix() + "queue";
        
        // 增加重试次数
        message.setRetryCount(message.getRetryCount() + 1);
        
        if (message.getRetryCount() <= mqConfig.getDelay().getRetry().getMaxRetries()) {
            // 未超过最大重试次数，延迟重试
            long retryDelayMs = calculateRetryDelay(message.getRetryCount());
            long nextDeliverTime = System.currentTimeMillis() + retryDelayMs;
            
            message.setDeliverTimestamp(nextDeliverTime);
            
            // 更新Redis中的消息
            redisTemplate.opsForValue().set(messageKey, JSON.toJSONString(message));
            
            // 更新有序集合中的分数（投递时间）
            redisTemplate.opsForZSet().add(queueKey, message.getId(), nextDeliverTime);
            
            log.info("延迟消息将重试: messageId={}, retryCount={}, nextDeliverTime={}, reason={}", 
                    message.getId(), message.getRetryCount(), 
                    Instant.ofEpochMilli(nextDeliverTime), reason);
        } else {
            // 超过最大重试次数，标记为失败
            message.setStatusEnum(MessageStatusEnum.FAILED);
            
            // 更新Redis中的消息状态
            redisTemplate.opsForValue().set(messageKey, JSON.toJSONString(message));
            
            // 从队列中移除
            redisTemplate.opsForZSet().remove(queueKey, message.getId());
            
            log.error("延迟消息投递失败，已达到最大重试次数: messageId={}, retryCount={}, reason={}", 
                    message.getId(), message.getRetryCount(), reason);
        }
    }

    /**
     * 计算重试延迟时间（指数退避策略）
     *
     * @param retryCount 重试次数
     * @return 延迟毫秒数
     */
    private long calculateRetryDelay(int retryCount) {
        // 获取配置的基础延迟时间和乘数
        long baseDelayMs = mqConfig.getDelay().getRetry().getRetryInterval();
        double multiplier = mqConfig.getDelay().getRetry().getRetryMultiplier();
        
        // 指数退避策略
        return (long) (baseDelayMs * Math.pow(multiplier, retryCount - 1));
    }

    /**
     * 定期清理过期的消息（每小时执行一次）
     */
    @Scheduled(cron = "0 0 * * * ?")
    public void cleanupExpiredMessages() {
        long now = System.currentTimeMillis();
        long threshold = now - mqConfig.getDelay().getMessageExpireTime();
        String queueKey = mqConfig.getDelay().getRedisKeyPrefix() + "queue";
        
        try {
            // 获取所有已投递或失败且超过清理阈值的消息
            Set<String> messageKeys = redisTemplate.keys(mqConfig.getDelay().getRedisKeyPrefix() + "*");
            if (messageKeys == null || messageKeys.isEmpty()) {
                return;
            }
            
            List<String> messagesToClean = messageKeys.stream()
                    .map(key -> redisTemplate.opsForValue().get(key))
                    .filter(json -> json != null)
                    .map(json -> JSON.parseObject(json, DelayMessage.class))
                    .filter(message -> {
                        MessageStatusEnum status = message.getStatusEnum();
                        return (status == MessageStatusEnum.DELIVERED || 
                                status == MessageStatusEnum.FAILED) && 
                                message.getDeliverTimestamp() < threshold;
                    })
                    .map(DelayMessage::getId)
                    .collect(Collectors.toList());
            
            if (!messagesToClean.isEmpty()) {
                // 删除过期消息
                for (String messageId : messagesToClean) {
                    String messageKey = mqConfig.getDelay().getRedisKeyPrefix() + messageId;
                    redisTemplate.delete(messageKey);
                    redisTemplate.opsForZSet().remove(queueKey, messageId);
                }
                
                log.info("已清理{}条过期延迟消息", messagesToClean.size());
            }
        } catch (Exception e) {
            log.error("清理过期延迟消息异常: {}", e.getMessage(), e);
        }
    }
}