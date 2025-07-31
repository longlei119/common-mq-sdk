package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.delay.adapter.ActiveMQAdapter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.core.JmsTemplate;

/**
 * ActiveMQ消息队列自动配置类
 */
@Configuration
@ConditionalOnClass(name = "javax.jms.ConnectionFactory")
public class ActiveMQAutoConfiguration {

    @Bean
    @ConditionalOnBean(JmsTemplate.class)
    @ConditionalOnMissingBean
    public ActiveMQAdapter activeMQAdapter(JmsTemplate jmsTemplate) {
        return new ActiveMQAdapter(jmsTemplate);
    }
}