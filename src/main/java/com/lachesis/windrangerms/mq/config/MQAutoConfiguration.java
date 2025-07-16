package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.MQConsumerManager;
import com.lachesis.windrangerms.mq.consumer.impl.ActiveMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RabbitMQConsumer;
import com.lachesis.windrangerms.mq.consumer.impl.RedisConsumer;
import com.lachesis.windrangerms.mq.producer.impl.EMQXProducer;
import com.lachesis.windrangerms.mq.consumer.impl.EMQXConsumer;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.delay.adapter.ActiveMQAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.KafkaAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.MQAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.RabbitMQAdapter;
import com.lachesis.windrangerms.mq.delay.adapter.RedisAdapter;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.producer.impl.ActiveMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.RabbitMQProducer;
import com.lachesis.windrangerms.mq.producer.impl.RedisProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.boot.env.YamlPropertySourceLoader;

@Slf4j
@Configuration
@EnableConfigurationProperties(MQConfig.class)
@EnableScheduling
@ComponentScan(basePackages = "com.lachesis.windrangerms.mq")
public class MQAutoConfiguration implements ApplicationRunner {

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
        @Bean
        @ConditionalOnBean(RabbitTemplate.class)
        @ConditionalOnMissingBean
        public RabbitMQAdapter rabbitMQAdapter(RabbitTemplate rabbitTemplate) {
            return new RabbitMQAdapter(rabbitTemplate);
        }
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
    }

    @ConditionalOnClass(name = "org.springframework.data.redis.core.StringRedisTemplate")
    @Configuration
    public static class DelayQueueAutoConfiguration {
        @Bean
        @ConditionalOnBean(StringRedisTemplate.class)
        @ConditionalOnProperty(prefix = "mq.delay", name = "enabled", havingValue = "true")
        @ConditionalOnMissingBean
        public DelayMessageSender delayMessageSender(StringRedisTemplate redisTemplate, List<MQAdapter> mqAdapters, MQConfig mqConfig) {
            return new DelayMessageSender(redisTemplate, mqAdapters, mqConfig);
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public MQFactory mqFactory(
            @Autowired(required = false) RedisProducer redisProducer,
            @Autowired(required = false) RedisConsumer redisConsumer,
            @Autowired(required = false) ActiveMQProducer activeMQProducer,
            @Autowired(required = false) ActiveMQConsumer activeMQConsumer,
            @Autowired(required = false) RabbitMQProducer rabbitMQProducer,
            @Autowired(required = false) RabbitMQConsumer rabbitMQConsumer,
            @Autowired(required = false) EMQXProducer emqxProducer,
            @Autowired(required = false) EMQXConsumer emqxConsumer
    ) {
        return new MQFactory(null, null, redisProducer, redisConsumer, activeMQProducer, activeMQConsumer, rabbitMQProducer, rabbitMQConsumer, emqxProducer, emqxConsumer);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("MQ自动配置启动完成");
    }

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
}