package com.example.mq.config;

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

    @Data
    public static class RedisProperties {
        private String host = "localhost";
        private Integer port = 6379;
        private String password;
        private Integer database = 0;
        private Integer timeout = 2000;

        private Pool pool = new Pool();

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
         * NameServer地址
         */
        private String nameServerAddr;

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

        @Data
        public static class ConsumerConfig {
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
}