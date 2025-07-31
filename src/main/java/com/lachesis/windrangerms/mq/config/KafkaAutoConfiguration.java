package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.impl.KafkaConsumer;
import com.lachesis.windrangerms.mq.delay.adapter.KafkaAdapter;
import com.lachesis.windrangerms.mq.producer.impl.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import java.util.Properties;

/**
 * Kafka自动配置类
 */
@Slf4j
@Configuration
@ConditionalOnClass(name = "org.apache.kafka.clients.producer.KafkaProducer")
@ConditionalOnProperty(prefix = "mq.kafka", name = "enabled", havingValue = "true")
public class KafkaAutoConfiguration {

    /**
     * 创建Kafka生产者
     */
    @Bean
    @ConditionalOnMissingBean
    public org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducerClient(MQConfig mqConfig) {
        Properties props = new Properties();
        MQConfig.KafkaProperties kafkaProperties = mqConfig.getKafka();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProperties.getProducerClientId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 生产者配置
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 等待所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 批量大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 延迟发送时间
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 缓冲区大小
        
        log.info("创建Kafka生产者: bootstrap-servers={}, client-id={}", 
                kafkaProperties.getBootstrapServers(), kafkaProperties.getProducerClientId());
        
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    /**
     * 创建Kafka生产者（用于延迟消息适配器）
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaProducerForAdapter")
    public org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducerForAdapter(MQConfig mqConfig) {
        Properties props = new Properties();
        MQConfig.KafkaProperties kafkaProperties = mqConfig.getKafka();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProperties.getProducerClientId() + "-adapter");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        
        // 生产者配置
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 等待所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 批量大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 延迟发送时间
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 缓冲区大小
        
        log.info("创建Kafka适配器生产者: bootstrap-servers={}, client-id={}", 
                kafkaProperties.getBootstrapServers(), kafkaProperties.getProducerClientId() + "-adapter");
        
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    /**
     * 创建Kafka消费者
     */
    @Bean
    @ConditionalOnMissingBean
    public org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumerClient(MQConfig mqConfig) {
        Properties props = new Properties();
        MQConfig.KafkaProperties kafkaProperties = mqConfig.getKafka();
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 消费者配置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早的消息开始消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 手动提交偏移量
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaProperties.getAutoCommitInterval());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 会话超时时间
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 心跳间隔
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // 每次拉取的最大记录数
        
        log.info("创建Kafka消费者: bootstrap-servers={}, group-id={}", 
                kafkaProperties.getBootstrapServers(), kafkaProperties.getConsumerGroupId());
        
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    /**
     * 创建Kafka生产者Bean
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaProducer kafkaProducer(org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducerClient) {
        log.info("创建KafkaProducer Bean");
        return new KafkaProducer(kafkaProducerClient);
    }

    /**
     * 创建Kafka消费者Bean
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaConsumer kafkaConsumer(org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumerClient) {
        log.info("创建KafkaConsumer Bean");
        return new KafkaConsumer(kafkaConsumerClient);
    }

    /**
     * 创建Kafka AdminClient用于管理topic
     */
    @Bean
    @ConditionalOnMissingBean
    public AdminClient kafkaAdminClient(MQConfig mqConfig) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, mqConfig.getKafka().getBootstrapServers());
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        
        log.info("创建Kafka AdminClient: bootstrap-servers={}", mqConfig.getKafka().getBootstrapServers());
        
        return AdminClient.create(configs);
    }

    /**
     * 创建Kafka适配器Bean
     */
    @Bean
    @ConditionalOnBean(name = "kafkaProducerForAdapter")
    @ConditionalOnMissingBean
    public KafkaAdapter kafkaAdapter(org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducerForAdapter) {
        log.info("创建KafkaAdapter Bean");
        return new KafkaAdapter(kafkaProducerForAdapter);
    }
}