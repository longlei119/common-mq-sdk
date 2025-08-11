package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.impl.ActiveMQConsumer;
import com.lachesis.windrangerms.mq.delay.adapter.ActiveMQAdapter;
import com.lachesis.windrangerms.mq.producer.impl.ActiveMQProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.core.JmsTemplate;
import javax.jms.ConnectionFactory;

/**
 * ActiveMQ消息队列自动配置类
 */
@Slf4j
@Configuration
@ConditionalOnClass(name = "javax.jms.ConnectionFactory")
@ConditionalOnProperty(prefix = "mq.activemq", name = "enabled", havingValue = "true")
public class ActiveMQAutoConfiguration {

    public ActiveMQAutoConfiguration() {
        log.info("ActiveMQAutoConfiguration 正在初始化...");
    }

    @Bean
    @Primary
    @ConditionalOnMissingBean
    @ConfigurationProperties(prefix = "spring.activemq")
    public ConnectionFactory activeMQConnectionFactory() {
        log.info("创建ActiveMQ ConnectionFactory");
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setTrustAllPackages(true);
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean
    public ActiveMQProducer activeMQProducer(JmsTemplate jmsTemplate) {
        log.info("创建ActiveMQProducer Bean，JmsTemplate: {}", jmsTemplate.getClass().getSimpleName());
        return new ActiveMQProducer(jmsTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public ActiveMQConsumer activeMQConsumer(JmsTemplate jmsTemplate) {
        log.info("创建ActiveMQConsumer Bean，JmsTemplate: {}", jmsTemplate.getClass().getSimpleName());
        return new ActiveMQConsumer(jmsTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public ActiveMQAdapter activeMQAdapter(JmsTemplate jmsTemplate) {
        log.info("创建ActiveMQAdapter Bean，JmsTemplate: {}", jmsTemplate.getClass().getSimpleName());
        return new ActiveMQAdapter(jmsTemplate);
    }
}