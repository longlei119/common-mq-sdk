package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.impl.ActiveMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.EMQXConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.KafkaConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RabbitMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RedisConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RocketMQConsumer;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.delay.adapter.MQAdapter;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.impl.ActiveMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.EMQXProducer;
import com.lachesis.windrangerms.mq.producer.impl.KafkaProducer;
import com.lachesis.windrangerms.mq.producer.impl.RabbitMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.RedisProducer;
import com.lachesis.windrangerms.mq.producer.impl.RocketMQProducer;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@Configuration
@EnableConfigurationProperties(MQConfig.class)
@EnableScheduling
@ComponentScan(basePackages = "com.lachesis.windrangerms.mq")
public class MQAutoConfiguration implements ApplicationRunner {



    /**
     * 引入死信队列自动配置
     */
    @Configuration
    @Import(DeadLetterAutoConfiguration.class)
    public static class DeadLetterConfiguration {
    }

    // MQ配置已分离到独立的AutoConfiguration类中

    @Bean
    @ConditionalOnBean(StringRedisTemplate.class)
    @ConditionalOnProperty(prefix = "mq.delay", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public DelayMessageSender delayMessageSender(StringRedisTemplate redisTemplate, @Autowired(required = false) List<MQAdapter> mqAdapters,
        MQConfig mqConfig) {
        if (mqAdapters == null || mqAdapters.isEmpty()) {
            log.warn("No MQAdapter beans found, DelayMessageSender will be created with empty adapter list");
            mqAdapters = new ArrayList<>();
        }
        log.info("Creating DelayMessageSender with {} MQAdapters", mqAdapters.size());
        return new DelayMessageSender(redisTemplate, mqAdapters, mqConfig);
    }

    @Bean
    @ConditionalOnMissingBean
    @Lazy
    public MQFactory mqFactory(
        @Autowired(required = false) RocketMQProducer rocketMQProducer,
        @Autowired(required = false) RocketMQConsumer rocketMQConsumer,
        @Autowired(required = false) RedisProducer redisProducer,
        @Autowired(required = false) RedisConsumer redisConsumer,
        @Autowired(required = false) ActiveMQProducer activeMQProducer,
        @Autowired(required = false) ActiveMQConsumer activeMQConsumer,
        @Autowired(required = false) RabbitMQProducer rabbitMQProducer,
        @Autowired(required = false) RabbitMQConsumer rabbitMQConsumer,
        @Autowired(required = false) EMQXProducer emqxProducer,
        @Autowired(required = false) EMQXConsumer emqxConsumer,
        @Autowired(required = false) KafkaProducer kafkaProducer,
        @Autowired(required = false) KafkaConsumer kafkaConsumer
    ) {
        return new MQFactory(rocketMQProducer, rocketMQConsumer, redisProducer, redisConsumer, activeMQProducer, activeMQConsumer, rabbitMQProducer,
            rabbitMQConsumer, emqxProducer, emqxConsumer, kafkaProducer, kafkaConsumer);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("MQ自动配置启动完成");
    }
}