package com.example.mq.rabbitmq;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootTest
public class BaseRabbitMQTest {

    @Configuration
    static class RabbitMQTestConfig {

        @Bean("rabbitConnectionFactory")
        public ConnectionFactory connectionFactory() {
            CachingConnectionFactory factory = new CachingConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");
            return factory;
        }

        @Bean
        public RabbitTemplate rabbitTemplate(ConnectionFactory rabbitConnectionFactory) {
            return new RabbitTemplate(rabbitConnectionFactory);
        }
    }
}