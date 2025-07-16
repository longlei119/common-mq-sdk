package com.lachesis.windrangerms.mq.delay.adapter;

import com.lachesis.windrangerms.mq.delay.model.DelayMessage;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Redis适配器实现
 * 使用Redis的发布订阅功能发送消息
 */
@Slf4j
public class RedisAdapter implements MQAdapter {

    private final StringRedisTemplate redisTemplate;

    public RedisAdapter(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        log.info("Redis适配器初始化成功");
    }

    @Override
    public boolean send(DelayMessage message) {
        try {
            // 确定Redis通道
            String channel = getChannel(message);
            
            // 发送消息到Redis通道
            redisTemplate.convertAndSend(channel, message.getBody());
            
            log.info("Redis消息发送成功，channel={}, messageId={}, body={}", 
                    channel, message.getId(), message.getBody());
            return true;
        } catch (Exception e) {
            log.error("Redis消息发送失败，messageId={}, error={}", message.getId(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * 获取Redis通道
     * 优先使用消息属性中的channel，否则使用topic
     */
    private String getChannel(DelayMessage message) {
        if (message.getProperties() != null && message.getProperties().containsKey("channel")) {
            return message.getProperties().get("channel");
        }
        return message.getTopic();
    }

    @Override
    public String getMQType() {
        return MQTypeEnum.REDIS.getType();
    }
}