package com.lachesis.windrangerms.mq.deadletter.config;

import lombok.Data;

/**
 * 死信队列配置类
 */
@Data
public class DeadLetterConfig {
    
    /**
     * 是否启用死信队列
     */
    private boolean enabled = true;
    
    /**
     * 存储类型：redis, mysql
     */
    private String storageType = "redis";
    
    /**
     * Redis配置
     */
    private RedisConfig redis = new RedisConfig();
    
    /**
     * MySQL配置
     */
    private MySQLConfig mysql = new MySQLConfig();
    
    /**
     * 重试配置
     */
    private RetryConfig retry = new RetryConfig();
    
    /**
     * 清理配置
     */
    private CleanupConfig cleanup = new CleanupConfig();
    
    /**
     * Redis配置
     */
    @Data
    public static class RedisConfig {
        /**
         * 死信消息存储的Redis键前缀
         */
        private String keyPrefix = "dead_letter:";
        
        /**
         * 死信队列键名
         */
        private String queueKey = "dead_letter:queue";
        
        /**
         * 死信消息过期时间（毫秒），默认30天
         */
        private long messageExpireTime = 30 * 24 * 60 * 60 * 1000L;
    }
    
    /**
     * MySQL配置
     */
    @Data
    public static class MySQLConfig {
        /**
         * 是否自动创建表
         */
        private boolean autoCreateTable = true;
        
        /**
         * 死信队列表名
         */
        private String deadLetterTableName = "mq_dead_letter_queue";
        
        /**
         * 重试历史表名
         */
        private String retryHistoryTableName = "mq_retry_history";
        
        /**
         * 数据源名称，为空则使用默认数据源
         */
        private String dataSourceName = "";
    }
    
    /**
     * 重试配置
     */
    @Data
    public static class RetryConfig {
        /**
         * 最大重试次数
         */
        private int maxRetries = 3;
        
        /**
         * 重试间隔（毫秒）
         */
        private int retryInterval = 60000; // 1分钟
        
        /**
         * 重试间隔倍数（指数退避策略）
         */
        private int retryMultiplier = 2;
    }
    
    /**
     * 清理配置
     */
    @Data
    public static class CleanupConfig {
        /**
         * 是否启用自动清理
         */
        private boolean enabled = true;
        
        /**
         * 清理周期（毫秒），默认1天
         */
        private long cleanupInterval = 24 * 60 * 60 * 1000L;
        
        /**
         * 消息保留时间（毫秒），默认30天
         */
        private long messageRetentionTime = 30 * 24 * 60 * 60 * 1000L;
    }
}