package com.example.mq.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.core.JmsTemplate;

@SpringBootTest
public class BaseActiveMQTest {

    @Configuration
    static class ActiveMQTestConfig {

        @Bean("activeMQConnectionFactory")
        public ActiveMQConnectionFactory connectionFactory() {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
            factory.setBrokerURL("tcp://localhost:61616");
            factory.setUserName("admin");
            factory.setPassword("admin");
            return factory;
        }

        @Bean
        public JmsTemplate jmsTemplate(ActiveMQConnectionFactory activeMQConnectionFactory) {
            JmsTemplate jmsTemplate = new JmsTemplate();
            jmsTemplate.setConnectionFactory(activeMQConnectionFactory);
            return jmsTemplate;
        }
    }
}