package com.lachesis.windrangerms.mq.broadcast;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多MQ广播测试
 * 测试各种MQ类型的广播功能
 */
@SpringBootTest
public class MultiMQBroadcastTest {

    private static final Logger logger = LoggerFactory.getLogger(MultiMQBroadcastTest.class);

    @Autowired
    private MQFactory mqFactory;

    @Autowired
    private MQConfig mqConfig;

    @Autowired
    private RabbitMQBroadcastTestConsumer rabbitMQBroadcastTestConsumer;

    @Autowired
    private RedisBroadcastTestConsumer redisBroadcastTestConsumer;

    @Autowired
    private KafkaBroadcastTestConsumer kafkaBroadcastTestConsumer;

    @BeforeEach
    public void setUp() {
        logger.info("开始多MQ广播测试");
    }

    /**
     * 测试RabbitMQ广播
     */
    @Test
    public void testRabbitMQBroadcast() throws InterruptedException {
        // 检查RabbitMQ是否启用
        if (mqConfig.getRabbitmq() == null || mqConfig.getRabbitmq().getHost() == null) {
            logger.info("RabbitMQ未启用，跳过测试");
            return;
        }

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch latch = new CountDownLatch(1);
        rabbitMQBroadcastTestConsumer.setLatch(latch);

        // 使用RabbitMQ生产者发送广播消息
        MQProducer rabbitMQProducer = mqFactory.getProducer(MQTypeEnum.RABBIT_MQ);
        logger.info("发送RabbitMQ广播消息，ID: {}", messageId);
        rabbitMQProducer.asyncSendBroadcast(MQTypeEnum.RABBIT_MQ, "test-broadcast-topic-rabbitmq", "test-tag", "这是一条RabbitMQ广播消息");

        // 等待消息被消费
        boolean await = latch.await(30, TimeUnit.SECONDS);
        assertTrue(await, "RabbitMQ广播消息未被消费");
        logger.info("RabbitMQ广播测试完成");
    }

    /**
     * 测试Redis广播
     */
    @Test
    public void testRedisBroadcast() throws InterruptedException {
        // 检查Redis是否启用
        if (mqConfig.getRedis() == null || mqConfig.getRedis().getHost() == null) {
            logger.info("Redis未启用，跳过测试");
            return;
        }

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch latch = new CountDownLatch(1);
        redisBroadcastTestConsumer.setLatch(latch);

        // 使用Redis生产者发送广播消息
        MQProducer redisProducer = mqFactory.getProducer(MQTypeEnum.REDIS);
        logger.info("发送Redis广播消息，ID: {}", messageId);
        redisProducer.asyncSendBroadcast(MQTypeEnum.REDIS, "test-broadcast-topic-redis", "test-tag", "这是一条Redis广播消息");

        // 等待消息被消费
        boolean await = latch.await(30, TimeUnit.SECONDS);
        assertTrue(await, "Redis广播消息未被消费");
        logger.info("Redis广播测试完成");
    }

    /**
     * 测试Kafka广播
     */
    @Test
    public void testKafkaBroadcast() throws InterruptedException {
        // 检查Kafka是否启用
        if (mqConfig.getKafka() == null || mqConfig.getKafka().getBootstrapServers() == null) {
            logger.info("Kafka未启用，跳过测试");
            return;
        }

        String messageId = UUID.randomUUID().toString().replace("-", "");
        Map<String, String> headers = new HashMap<>();
        headers.put("messageId", messageId);

        CountDownLatch latch = new CountDownLatch(1);
        kafkaBroadcastTestConsumer.setLatch(latch);

        // 使用Kafka生产者发送广播消息
        MQProducer kafkaProducer = mqFactory.getProducer(MQTypeEnum.KAFKA);
        logger.info("发送Kafka广播消息，ID: {}", messageId);
        kafkaProducer.asyncSendBroadcast(MQTypeEnum.KAFKA, "test-broadcast-topic-kafka", "test-tag", "这是一条Kafka广播消息");

        // 等待消息被消费
        boolean await = latch.await(30, TimeUnit.SECONDS);
        assertTrue(await, "Kafka广播消息未被消费");
        logger.info("Kafka广播测试完成");
    }

    @Component
    public static class RabbitMQBroadcastTestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(RabbitMQBroadcastTestConsumer.class);
        private CountDownLatch latch;

        @MQConsumer(topic = "test-broadcast-topic-rabbitmq", tag = "test-tag", mqType = MQTypeEnum.RABBIT_MQ)
        public void consume(String message) {
            logger.info("RabbitMQBroadcastTestConsumer 接收到广播消息: {}", message);
            if (latch != null) {
                latch.countDown();
            }
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }

    @Component
    public static class RedisBroadcastTestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(RedisBroadcastTestConsumer.class);
        private CountDownLatch latch;

        @MQConsumer(topic = "test-broadcast-topic-redis", tag = "test-tag", mqType = MQTypeEnum.REDIS)
        public void consume(String message) {
            logger.info("RedisBroadcastTestConsumer 接收到广播消息: {}", message);
            if (latch != null) {
                latch.countDown();
            }
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }

    @Component
    public static class KafkaBroadcastTestConsumer {

        private static final Logger logger = LoggerFactory.getLogger(KafkaBroadcastTestConsumer.class);
        private CountDownLatch latch;

        @MQConsumer(topic = "test-broadcast-topic-kafka", tag = "test-tag", mqType = MQTypeEnum.KAFKA)
        public void consume(String message) {
            logger.info("KafkaBroadcastTestConsumer 接收到广播消息: {}", message);
            if (latch != null) {
                latch.countDown();
            }
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }
    }
}