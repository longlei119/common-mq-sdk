package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.impl.ActiveMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.EMQXConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RabbitMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RedisConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RocketMQConsumer;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.delay.adapter.ActiveMQAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.EMQXAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.KafkaAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.MQAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.RabbitMQAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.RedisAdapter;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.impl.ActiveMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.EMQXProducer;
import com.lachesis.windrangerms.mq.producer.impl.RabbitMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.RedisProducer;
import com.lachesis.windrangerms.mq.producer.impl.RocketMQProducer;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.jms.core.JmsTemplate;
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
    public static class DeadLetterConfiguration {}
    

    // 其它 MQ 配置同前

    @ConditionalOnClass(name = "javax.jms.ConnectionFactory")
    @Configuration
    public static class ActiveMQAutoConfiguration {
        @Bean
        @ConditionalOnBean(JmsTemplate.class)
        @ConditionalOnMissingBean
        public ActiveMQAdapter activeMQAdapter(JmsTemplate jmsTemplate) {
            return new ActiveMQAdapter(jmsTemplate);
        }
    }

    @ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
    @Configuration
    public static class RabbitMQAutoConfiguration {

        

    }

    @ConditionalOnClass(name = "org.apache.kafka.clients.producer.KafkaProducer")
    @Configuration
    public static class KafkaAutoConfiguration {
        @Bean
        @ConditionalOnBean(KafkaProducer.class)
        @ConditionalOnMissingBean
        public KafkaAdapter kafkaAdapter(KafkaProducer<String, byte[]> kafkaProducer) {
            return new KafkaAdapter(kafkaProducer);
        }
    }

    @ConditionalOnClass(name = "org.eclipse.paho.client.mqttv3.MqttClient")
    @Configuration
    public static class EMQXAutoConfiguration {
        @Bean
        @ConditionalOnBean(MqttClient.class)
        @ConditionalOnMissingBean
        public EMQXAdapter emqxAdapter(MqttClient mqttClient) {
            return new EMQXAdapter(mqttClient);
        }
    }

    @ConditionalOnClass(name = "org.springframework.data.redis.core.StringRedisTemplate")
    @Configuration
    public static class RedisAutoConfiguration {
        @Bean
        @ConditionalOnProperty(prefix = "mq.redis", name = "host")
        @ConditionalOnMissingBean
        public RedisAdapter redisAdapter(StringRedisTemplate redisTemplate) {
            return new RedisAdapter(redisTemplate);
        }
        // 新增 RedisConsumer Bean
        @Bean
        @ConditionalOnProperty(prefix = "mq.redis", name = "host")
        @ConditionalOnMissingBean
        public RedisConsumer redisConsumer(RedisMessageListenerContainer listenerContainer) {
            return new RedisConsumer(listenerContainer);
        }
        // 新增 RedisProducer Bean
        @Bean
        @ConditionalOnProperty(prefix = "mq.redis", name = "host")
        @ConditionalOnMissingBean
        public RedisProducer redisProducer(StringRedisTemplate redisTemplate, @Autowired(required = false) DelayMessageSender delayMessageSender) {
            return new RedisProducer(redisTemplate, delayMessageSender);
        }
    }

    @Bean
    @ConditionalOnBean(StringRedisTemplate.class)
    @ConditionalOnProperty(prefix = "mq.delay", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public DelayMessageSender delayMessageSender(StringRedisTemplate redisTemplate, @Autowired(required = false) List<MQAdapter> mqAdapters, MQConfig mqConfig) {
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
            @Autowired(required = false) EMQXConsumer emqxConsumer
    ) {
        return new MQFactory(rocketMQProducer, rocketMQConsumer, redisProducer, redisConsumer, activeMQProducer, activeMQConsumer, rabbitMQProducer, rabbitMQConsumer, emqxProducer, emqxConsumer);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("MQ自动配置启动完成");
        log.info("RabbitMQ配置: enabled={}, host={}, port={}", 
                mqConfig.getRabbitmq().isEnabled(),
                mqConfig.getRabbitmq().getHost(),
                mqConfig.getRabbitmq().getPort());
    }
    
    @Autowired
    private MQConfig mqConfig;

    @Bean
    public static ApplicationListener<ApplicationEnvironmentPreparedEvent> environmentPreparedEventApplicationListener() {
        return event -> {
            ConfigurableEnvironment environment = event.getEnvironment();
            try {
                ClassPathResource resource = new ClassPathResource("application-mq.yml");
                if (resource.exists()) {
                    YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
                    for (PropertySource<?> propertySource : loader.load("application-mq", resource)) {
                        environment.getPropertySources().addLast(propertySource);
                    }
                }
            } catch (Exception e) {
                // 可以根据需要添加日志
            }
        };
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public ConnectionFactory rabbitConnectionFactory(MQConfig mqConfig) {
        log.info("创建RabbitMQ ConnectionFactory, enabled={}, host={}, port={}", 
                mqConfig.getRabbitmq().isEnabled(), 
                mqConfig.getRabbitmq().getHost(), 
                mqConfig.getRabbitmq().getPort());
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost(mqConfig.getRabbitmq().getHost());
        connectionFactory.setPort(mqConfig.getRabbitmq().getPort());
        connectionFactory.setUsername(mqConfig.getRabbitmq().getUsername());
        connectionFactory.setPassword(mqConfig.getRabbitmq().getPassword());
        connectionFactory.setVirtualHost(mqConfig.getRabbitmq().getVirtualHost());
        return connectionFactory;
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        log.info("创建RabbitTemplate Bean");
        return new RabbitTemplate(connectionFactory);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public org.springframework.amqp.rabbit.core.RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new org.springframework.amqp.rabbit.core.RabbitAdmin(connectionFactory);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true")
    @ConditionalOnBean(RabbitTemplate.class)
    @ConditionalOnMissingBean
    public RabbitMQProducer rabbitMQProducer(RabbitTemplate rabbitTemplate) {
        log.info("创建RabbitMQProducer Bean");
        return new RabbitMQProducer(rabbitTemplate);
    }
    
    @Bean
    @ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true")
    @ConditionalOnBean(RabbitTemplate.class)
    @ConditionalOnMissingBean
    public RabbitMQConsumer rabbitMQConsumer(RabbitTemplate rabbitTemplate) {
        log.info("创建RabbitMQConsumer Bean");
        return new RabbitMQConsumer(rabbitTemplate);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public RabbitMQAdapter rabbitMQAdapter(RabbitTemplate rabbitTemplate) {
        log.info("创建RabbitMQAdapter Bean");
        return new RabbitMQAdapter(rabbitTemplate);
    }
}