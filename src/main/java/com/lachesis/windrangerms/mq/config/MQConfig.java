package com.lachesis.windrangerms.mq.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 消息队列配置类
 */
@Data
@ConfigurationProperties(prefix = "mq")
public class MQConfig {

    /**
     * Redis配置
     */
    private RedisProperties redis = new RedisProperties();

    /**
     * RocketMQ配置
     */
    private RocketMQProperties rocketmq = new RocketMQProperties();

    /**
     * Kafka配置
     */
    private KafkaProperties kafka = new KafkaProperties();

    /**
     * RabbitMQ配置
     */
    private RabbitMQProperties rabbitmq = new RabbitMQProperties();

    /**
     * EMQX配置
     */
    private EMQXProperties emqx = new EMQXProperties();

    /**
     * ActiveMQ配置
     */
    private ActiveMQProperties activemq = new ActiveMQProperties();

    /**
     * 延迟消息配置
     */
    private DelayMessageProperties delay = new DelayMessageProperties();

    /**
     * 死信队列配置
     */
    private DeadLetterProperties deadLetter = new DeadLetterProperties();

    /**
     * 获取死信队列配置
     */
    public DeadLetterProperties getDeadLetter() {
        return deadLetter;
    }

    @Data
    public static class RedisProperties {
        private String host = "localhost";
        private Integer port = 6379;
        private String password;
        private Integer database = 0;
        private Integer timeout = 2000;

        private Pool pool = new Pool();

        public boolean isEnabled = true;

        @Data
        public static class Pool {
            private Integer maxActive = 8;
            private Integer maxIdle = 8;
            private Integer minIdle = 0;
            private Integer maxWait = -1;
        }
    }

    @Data
    public static class RocketMQProperties {

        /**
         * 获取消费者配置
         */
        public ConsumerConfig getConsumer() {
            return consumer;
        }

        /**
         * NameServer地址
         */
        private String nameServerAddr;

        /**
         * 获取NameServer地址
         */
        public String getNameServerAddr() {
            return nameServerAddr;
        }

        /**
         * 生产者组
         */
        private String producerGroup;

        /**
         * 消费者组
         */
        private String consumerGroup;

        /**
         * 发送消息超时时间（毫秒）
         */
        private int sendMsgTimeout = 3000;

        /**
         * 消费者配置
         */
        private ConsumerConfig consumer = new ConsumerConfig();

        private boolean isEnabled = true;

        @Data
        public static class ConsumerConfig {

            /**
             * 获取消费者组名
             */
            public String getGroupName() {
                return groupName;
            }

            /**
             * 获取消费者最小线程数
             */
            public int getThreadMin() {
                return threadMin;
            }

            /**
             * 获取消费者最大线程数
             */
            public int getThreadMax() {
                return threadMax;
            }

            /**
             * 获取批量消费最大消息数
             */
            public int getBatchMaxSize() {
                return batchMaxSize;
            }

            /**
             * 消费者组名
             */
            private String groupName = "default-consumer-group";

            /**
             * 消费者最小线程数
             */
            private int threadMin = 20;

            /**
             * 消费者最大线程数
             */
            private int threadMax = 64;

            /**
             * 批量消费最大消息数
             */
            private int batchMaxSize = 1;

            /**
             * 消费超时时间（毫秒）
             */
            private int consumeTimeout = 15000;

            /**
             * 获取消费超时时间
             */
            public int getConsumeTimeout() {
                return consumeTimeout;
            }

            /**
             * 消费起始位置
             */
            private org.apache.rocketmq.common.consumer.ConsumeFromWhere consumeFromWhere =
                org.apache.rocketmq.common.consumer.ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

            /**
             * 获取消费起始位置
             */
            public org.apache.rocketmq.common.consumer.ConsumeFromWhere getConsumeFromWhere() {
                return consumeFromWhere;
            }
        }
    }

    @Data
    public static class KafkaProperties {
        /**
         * 服务器地址
         */
        private String bootstrapServers;

        /**
         * 生产者客户端ID
         */
        private String producerClientId;

        /**
         * 消费者组ID
         */
        private String consumerGroupId;

        /**
         * 自动提交间隔（毫秒）
         */
        private int autoCommitInterval = 1000;
    }

    @Data
    public static class RabbitMQProperties {
        /**
         * 是否启用RabbitMQ
         */
        private boolean enabled = false;

        /**
         * 服务器地址
         */
        private String host;

        /**
         * 端口
         */
        private int port = 5672;

        /**
         * 用户名
         */
        private String username;

        /**
         * 密码
         */
        private String password;

        /**
         * 虚拟主机
         */
        private String virtualHost = "/";
    }

    @Data
    public static class EMQXProperties {
        /**
         * 是否启用EMQX
         */
        private boolean enabled = false;

        /**
         * 服务器地址
         */
        private String serverUri = "tcp://localhost:1883";

        /**
         * 用户名
         */
        private String username = "admin";

        /**
         * 密码
         */
        private String password = "public";

        /**
         * 客户端ID
         */
        private String clientId = "testClient";

        /**
         * 清除会话
         */
        private boolean cleanSession = true;

        /**
         * 心跳间隔（秒）
         */
        private int keepAliveInterval = 60;
    }

    @Data
    public static class ActiveMQProperties {
        /**
         * 服务器地址
         */
        private String brokerUrl = "tcp://localhost:61616";

        /**
         * 用户名
         */
        private String username = "admin";

        /**
         * 密码
         */
        private String password = "admin";

        /**
         * 连接池配置
         */
        private Pool pool = new Pool();

        @Data
        public static class Pool {
            /**
             * 最大连接数
             */
            private int maxConnections = 10;

            /**
             * 空闲连接超时时间（毫秒）
             */
            private int idleTimeout = 30000;
        }
    }

    @Data
    public static class DelayMessageProperties {
        /**
         * 是否启用延迟消息功能
         */
        private boolean enabled = true;

        /**
         * 延迟消息存储的Redis键前缀
         */
        private String redisKeyPrefix = "delay_message:";

        /**
         * 延迟消息扫描间隔（毫秒）
         */
        private int scanInterval = 1000;

        /**
         * 每次扫描处理的最大消息数
         */
        private int batchSize = 100;

        /**
         * 消息重试配置
         */
        private RetryConfig retry = new RetryConfig();

        /**
         * 消息过期时间（毫秒），默认7天
         */
        private long messageExpireTime = 7 * 24 * 60 * 60 * 1000L;

        /**
         * 默认的消息队列类型，当未指定时使用
         */
        private String defaultMQType = "ROCKET_MQ";

        /**
         * 消息队列类型与延迟级别的映射
         * 例如：RocketMQ的延迟级别为1s/5s/10s/30s/1m/2m/3m/4m/5m/6m/7m/8m/9m/10m/20m/30m/1h/2h
         */
        private Map<String, Integer> delayLevelMapping = new HashMap<>();

        @Data
        public static class RetryConfig {
            /**
             * 最大重试次数
             */
            private int maxRetries = 3;

            /**
             * 重试间隔（毫秒）
             */
            private int retryInterval = 5000;

            /**
             * 重试间隔倍数（指数退避策略）
             */
            private int retryMultiplier = 2;
        }
    }

    @Data
    public static class DeadLetterProperties {

        /**
         * 获取死信队列配置
         */
        public DeadLetterProperties getDeadLetter() {
            return this;
        }

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
         * 扫描间隔（毫秒）
         */
        private int scanInterval = 5000;

        /**
         * 每次扫描处理的最大消息数
         */
        private int batchSize = 100;

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
}