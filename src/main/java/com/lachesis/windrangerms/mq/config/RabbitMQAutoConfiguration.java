package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.impl.RabbitMQConsumer;
import com.lachesis.windrangerms.mq.delay.adapter.RabbitMQAdapter;
import com.lachesis.windrangerms.mq.producer.impl.RabbitMQProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ消息队列自动配置类
 */
@Slf4j
@Configuration
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
public class RabbitMQAutoConfiguration {

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