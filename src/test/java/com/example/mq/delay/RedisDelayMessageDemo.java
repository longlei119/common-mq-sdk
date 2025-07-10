package com.example.mq.delay;

import com.example.mq.config.MQConfig;
import com.example.mq.delay.adapter.RedisAdapter;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Redis延迟消息演示
 * 展示Redis延迟消息的各种使用场景
 */
@Slf4j
public class RedisDelayMessageDemo {

    public static void main(String[] args) throws Exception {
        log.info("=== Redis延迟消息功能演示开始 ===");
        
        RedisDelayMessageDemo demo = new RedisDelayMessageDemo();
        
        // 演示1：缓存过期通知
        demo.demonstrateCacheExpirationNotification();
        
        // 演示2：会话超时处理
        demo.demonstrateSessionTimeoutHandling();
        
        // 演示3：数据同步通知
        demo.demonstrateDataSyncNotification();
        
        // 演示4：实时统计更新
        demo.demonstrateRealtimeStatisticsUpdate();
        
        // 演示5：Redis适配器功能特性
        demo.demonstrateRedisAdapterFeatures();
        
        log.info("=== Redis延迟消息功能演示结束 ===");
    }

    /**
     * 演示1：缓存过期通知
     * 模拟缓存数据过期时的通知机制
     */
    public void demonstrateCacheExpirationNotification() throws Exception {
        log.info("\n--- 演示1：缓存过期通知 ---");
        
        DelayMessageSender sender = createDelayMessageSender();
        
        // 场景：用户缓存数据即将过期，提前5秒通知清理
        String topic = "cache.expiration";
        String tag = "user.cache";
        String cacheKey = "user:12345:profile";
        String body = String.format("缓存即将过期，请及时清理：%s", cacheKey);
        String mqType = MQTypeEnum.REDIS.getType();
        long delayTime = 5000; // 5秒后通知
        
        log.info("发送缓存过期通知消息：cacheKey={}, delayTime={}ms", cacheKey, delayTime);
        String messageId = sender.sendDelayMessage(topic, tag, body, mqType, delayTime);
        log.info("缓存过期通知消息已发送，messageId={}", messageId);
        
        // 模拟缓存清理处理
        log.info("缓存清理逻辑：检查缓存状态 -> 执行清理操作 -> 记录清理日志");
    }

    /**
     * 演示2：会话超时处理
     * 模拟用户会话超时的延迟处理
     */
    public void demonstrateSessionTimeoutHandling() throws Exception {
        log.info("\n--- 演示2：会话超时处理 ---");
        
        DelayMessageSender sender = createDelayMessageSender();
        
        // 场景：用户会话30分钟后自动超时
        DelayMessage sessionMessage = new DelayMessage();
        sessionMessage.setId("session-timeout-" + System.currentTimeMillis());
        sessionMessage.setTopic("session.timeout");
        sessionMessage.setTag("user.session");
        sessionMessage.setBody("用户会话即将超时，请保存数据");
        sessionMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        sessionMessage.setDeliverTimestamp(System.currentTimeMillis() + 30 * 60 * 1000); // 30分钟后
        
        // 设置会话相关属性
        Map<String, String> properties = new HashMap<>();
        properties.put("userId", "user12345");
        properties.put("sessionId", "sess_abc123def456");
        properties.put("channel", "session.notification.channel");
        properties.put("timeout", "1800"); // 30分钟
        sessionMessage.setProperties(properties);
        
        log.info("发送会话超时消息：userId={}, sessionId={}, timeout={}分钟", 
                properties.get("userId"), properties.get("sessionId"), 
                Integer.parseInt(properties.get("timeout")) / 60);
        String messageId = sender.sendDelayMessage(sessionMessage);
        log.info("会话超时消息已发送，messageId={}", messageId);
        
        // 模拟会话超时处理
        log.info("会话超时处理逻辑：检查会话状态 -> 保存用户数据 -> 清理会话信息 -> 通知用户");
    }

    /**
     * 演示3：数据同步通知
     * 模拟数据同步的延迟通知机制
     */
    public void demonstrateDataSyncNotification() throws Exception {
        log.info("\n--- 演示3：数据同步通知 ---");
        
        DelayMessageSender sender = createDelayMessageSender();
        
        // 场景：数据变更后，延迟10秒进行同步通知
        DelayMessage syncMessage = new DelayMessage();
        syncMessage.setId("data-sync-" + UUID.randomUUID().toString());
        syncMessage.setTopic("data.sync");
        syncMessage.setTag("database.change");
        syncMessage.setBody("数据已变更，需要同步到其他系统");
        syncMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        syncMessage.setDeliverTimestamp(System.currentTimeMillis() + 10000); // 10秒后
        
        // 设置同步相关属性
        Map<String, String> properties = new HashMap<>();
        properties.put("tableId", "user_profile");
        properties.put("operation", "UPDATE");
        properties.put("recordId", "12345");
        properties.put("channel", "data.sync.channel");
        properties.put("priority", "high");
        properties.put("syncTarget", "elasticsearch,redis,cache");
        syncMessage.setProperties(properties);
        
        log.info("发送数据同步通知：table={}, operation={}, recordId={}, targets={}", 
                properties.get("tableId"), properties.get("operation"), 
                properties.get("recordId"), properties.get("syncTarget"));
        String messageId = sender.sendDelayMessage(syncMessage);
        log.info("数据同步通知已发送，messageId={}", messageId);
        
        // 模拟数据同步处理
        log.info("数据同步处理逻辑：读取变更数据 -> 转换数据格式 -> 同步到目标系统 -> 记录同步状态");
    }

    /**
     * 演示4：实时统计更新
     * 模拟实时统计数据的延迟更新
     */
    public void demonstrateRealtimeStatisticsUpdate() throws Exception {
        log.info("\n--- 演示4：实时统计更新 ---");
        
        DelayMessageSender sender = createDelayMessageSender();
        
        // 场景：用户行为统计，每5秒批量更新一次
        DelayMessage statsMessage = new DelayMessage();
        statsMessage.setId("stats-update-" + System.currentTimeMillis());
        statsMessage.setTopic("statistics.update");
        statsMessage.setTag("user.behavior");
        statsMessage.setBody("批量更新用户行为统计数据");
        statsMessage.setMqTypeEnum(MQTypeEnum.REDIS);
        statsMessage.setDeliverTimestamp(System.currentTimeMillis() + 5000); // 5秒后
        
        // 设置统计相关属性
        Map<String, String> properties = new HashMap<>();
        properties.put("statsType", "user_behavior");
        properties.put("timeWindow", "5s");
        properties.put("batchSize", "1000");
        properties.put("channel", "stats.update.channel");
        properties.put("metrics", "pv,uv,click,conversion");
        properties.put("aggregation", "sum,count,avg");
        statsMessage.setProperties(properties);
        
        log.info("发送统计更新消息：type={}, window={}, batchSize={}, metrics={}", 
                properties.get("statsType"), properties.get("timeWindow"), 
                properties.get("batchSize"), properties.get("metrics"));
        String messageId = sender.sendDelayMessage(statsMessage);
        log.info("统计更新消息已发送，messageId={}", messageId);
        
        // 模拟统计更新处理
        log.info("统计更新处理逻辑：收集行为数据 -> 计算统计指标 -> 更新统计表 -> 刷新缓存");
    }

    /**
     * 演示5：Redis适配器功能特性
     * 展示Redis适配器的各种功能特性
     */
    public void demonstrateRedisAdapterFeatures() throws Exception {
        log.info("\n--- 演示5：Redis适配器功能特性 ---");
        
        // 创建Redis适配器
        RedisAdapter redisAdapter = createRedisAdapter();
        
        // 特性1：发布订阅消息
        log.info("特性1：Redis发布订阅消息");
        DelayMessage pubsubMessage = createSampleMessage("pubsub.demo", "notification", "Redis发布订阅演示消息");
        pubsubMessage.getProperties().put("channel", "demo.notification.channel");
        boolean pubsubResult = redisAdapter.send(pubsubMessage);
        log.info("发布订阅消息发送结果：{}", pubsubResult ? "成功" : "失败");
        
        // 特性2：列表操作消息
        log.info("特性2：Redis列表操作消息");
        DelayMessage listMessage = createSampleMessage("list.demo", "queue", "Redis列表操作演示消息");
        listMessage.getProperties().put("channel", "demo.list.channel");
        listMessage.getProperties().put("list-key", "demo:message:queue");
        listMessage.getProperties().put("operation", "lpush");
        boolean listResult = redisAdapter.send(listMessage);
        log.info("列表操作消息发送结果：{}", listResult ? "成功" : "失败");
        
        // 特性3：缓存通知消息
        log.info("特性3：Redis缓存通知消息");
        DelayMessage cacheMessage = createSampleMessage("cache.demo", "invalidation", "Redis缓存失效演示消息");
        cacheMessage.getProperties().put("channel", "demo.cache.channel");
        cacheMessage.getProperties().put("cache-key", "demo:cache:user:12345");
        cacheMessage.getProperties().put("cache-ttl", "3600");
        boolean cacheResult = redisAdapter.send(cacheMessage);
        log.info("缓存通知消息发送结果：{}", cacheResult ? "成功" : "失败");
        
        // 特性4：获取MQ类型
        log.info("特性4：获取Redis适配器MQ类型");
        String mqType = redisAdapter.getMQType();
        log.info("Redis适配器MQ类型：{}", mqType);
        
        // 特性5：无通道消息（使用topic作为通道）
        log.info("特性5：无通道消息处理");
        DelayMessage noChannelMessage = createSampleMessage("default.channel.demo", "auto", "自动通道演示消息");
        // 不设置channel属性，将使用topic作为通道
        boolean noChannelResult = redisAdapter.send(noChannelMessage);
        log.info("无通道消息发送结果：{}", noChannelResult ? "成功" : "失败");
        
        log.info("Redis适配器功能特性演示完成");
    }

    /**
     * 创建延迟消息发送器
     */
    private DelayMessageSender createDelayMessageSender() throws Exception {
        // 创建模拟的Redis模板
        StringRedisTemplate redisTemplate = new StringRedisTemplate();
        
        // 创建Redis适配器
        RedisAdapter redisAdapter = createRedisAdapter();
        
        // 创建适配器列表
        List<com.example.mq.delay.adapter.MQAdapter> adapters = new ArrayList<>();
        adapters.add(redisAdapter);
        
        // 创建配置
        MQConfig mqConfig = createMQConfig();
        
        return new DelayMessageSender(redisTemplate, adapters, mqConfig);
    }

    /**
     * 创建Redis适配器
     */
    private RedisAdapter createRedisAdapter() {
        StringRedisTemplate messageRedisTemplate = new StringRedisTemplate();
        return new RedisAdapter(messageRedisTemplate);
    }

    /**
     * 创建MQ配置
     */
    private MQConfig createMQConfig() {
        MQConfig mqConfig = new MQConfig();
        MQConfig.DelayMessageProperties delayProps = new MQConfig.DelayMessageProperties();
        delayProps.setRedisKeyPrefix("demo_delay_message:");
        delayProps.setScanInterval(100);
        delayProps.setBatchSize(10);
        delayProps.setMessageExpireTime(7 * 24 * 60 * 60 * 1000L); // 7天
        
        // 重试配置
        MQConfig.DelayMessageProperties.RetryConfig retryConfig = new MQConfig.DelayMessageProperties.RetryConfig();
        retryConfig.setMaxRetries(3);
        retryConfig.setRetryInterval(1000);
        retryConfig.setRetryMultiplier(2);
        delayProps.setRetry(retryConfig);
        
        mqConfig.setDelay(delayProps);
        return mqConfig;
    }

    /**
     * 创建示例消息
     */
    private DelayMessage createSampleMessage(String topic, String tag, String body) {
        DelayMessage message = new DelayMessage();
        message.setId("demo-msg-" + System.currentTimeMillis());
        message.setTopic(topic);
        message.setTag(tag);
        message.setBody(body);
        message.setMqTypeEnum(MQTypeEnum.REDIS);
        message.setDeliverTimestamp(System.currentTimeMillis() + 1000); // 1秒后投递
        
        // 设置基本属性
        Map<String, String> properties = new HashMap<>();
        properties.put("source", "demo");
        properties.put("version", "1.0");
        properties.put("timestamp", String.valueOf(System.currentTimeMillis()));
        message.setProperties(properties);
        
        return message;
    }
}