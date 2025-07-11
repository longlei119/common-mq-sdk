package com.example.mq.config;

import com.example.mq.consumer.impl.ActiveMQConsumer;
import com.example.mq.consumer.impl.RabbitMQConsumer;
import com.example.mq.consumer.impl.RocketMQConsumer;
import com.example.mq.producer.impl.EMQXProducer;
import com.example.mq.consumer.impl.EMQXConsumer;
import com.example.mq.delay.DelayMessageSender;
import com.example.mq.delay.adapter.ActiveMQAdapter;
import com.example.mq.delay.adapter.KafkaAdapter;
import com.example.mq.delay.adapter.MQAdapter;
import com.example.mq.delay.adapter.RabbitMQAdapter;
import com.example.mq.delay.adapter.RocketMQAdapter;
import com.example.mq.factory.MQFactory;
import com.example.mq.producer.impl.ActiveMQProducer;
import com.example.mq.producer.impl.RabbitMQProducer;
import com.example.mq.producer.impl.RocketMQProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
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

@Configuration
@EnableConfigurationProperties(MQConfig.class)
@EnableScheduling
public class MQAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
    @ConditionalOnMissingBean
    public DefaultMQProducer rocketMQDefaultProducer(MQConfig mqConfig) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr(mqConfig.getRocketmq().getNameServerAddr());
        producer.setProducerGroup(mqConfig.getRocketmq().getProducerGroup());
        producer.setSendMsgTimeout(mqConfig.getRocketmq().getSendMsgTimeout());
        producer.start();
        return producer;
    }

    @Bean
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean
    public RocketMQProducer rocketMQProducer(DefaultMQProducer defaultMQProducer) {
        return new RocketMQProducer(defaultMQProducer);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
    @ConditionalOnMissingBean
    public DefaultMQPushConsumer rocketMQDefaultPushConsumer(MQConfig mqConfig) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr(mqConfig.getRocketmq().getNameServerAddr());
        consumer.setConsumerGroup(mqConfig.getRocketmq().getConsumerGroup());
        return consumer;
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
    @ConditionalOnBean(DefaultMQPushConsumer.class)
    @ConditionalOnMissingBean
    public RocketMQConsumer rocketMQConsumer(DefaultMQPushConsumer defaultMQPushConsumer, MQConfig.RocketMQProperties rocketMQProperties) {
        return new RocketMQConsumer(defaultMQPushConsumer, rocketMQProperties);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
    @ConditionalOnMissingBean
    public MQConfig.RocketMQProperties rocketMQProperties(MQConfig mqConfig) {
        return mqConfig.getRocketmq();
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "host")
    @ConditionalOnMissingBean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        return new StringRedisTemplate(redisConnectionFactory);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "host")
    @ConditionalOnMissingBean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory redisConnectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        return container;
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public MqttPahoClientFactory mqttClientFactory(MQConfig mqConfig) {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{mqConfig.getEmqx().getServerUri()});
        options.setUserName(mqConfig.getEmqx().getUsername());
        options.setPassword(mqConfig.getEmqx().getPassword().toCharArray());
        options.setCleanSession(mqConfig.getEmqx().isCleanSession());
        options.setKeepAliveInterval(mqConfig.getEmqx().getKeepAliveInterval());
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
    @ConditionalOnBean(MqttPahoClientFactory.class)
    @ConditionalOnMissingBean
    public MqttClient mqttClient(MQConfig mqConfig, MqttPahoClientFactory mqttClientFactory) throws Exception {
        MqttClient client = (MqttClient) mqttClientFactory.getClientInstance(
                mqConfig.getEmqx().getServerUri(),
                mqConfig.getEmqx().getClientId());
        client.connect(mqttClientFactory.getConnectionOptions());
        return client;
    }

    @Bean
    @ConditionalOnBean(MqttClient.class)
    @ConditionalOnMissingBean
    public EMQXProducer emqxProducer(MqttClient mqttClient) {
        return new EMQXProducer(mqttClient);
    }

    @Bean
    @ConditionalOnBean(MqttClient.class)
    @ConditionalOnMissingBean
    public EMQXConsumer emqxConsumer(MqttClient mqttClient) {
        return new EMQXConsumer(mqttClient);
    }

    @Bean("rabbitConnectionFactory")
    @ConditionalOnProperty(prefix = "mq.rabbitmq", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean(name = "rabbitConnectionFactory")
    public ConnectionFactory rabbitConnectionFactory(MQConfig mqConfig) {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        MQConfig.RabbitMQProperties rabbitMQProps = mqConfig.getRabbitmq();
        factory.setHost(rabbitMQProps.getHost());
        factory.setPort(rabbitMQProps.getPort());
        factory.setUsername(rabbitMQProps.getUsername());
        factory.setPassword(rabbitMQProps.getPassword());
        if (rabbitMQProps.getVirtualHost() != null) {
            factory.setVirtualHost(rabbitMQProps.getVirtualHost());
        }
        return factory;
    }

    @Bean
    @ConditionalOnBean(name = "rabbitConnectionFactory")
    @ConditionalOnMissingBean
    public RabbitTemplate rabbitTemplate(ConnectionFactory rabbitConnectionFactory) {
        return new RabbitTemplate(rabbitConnectionFactory);
    }

    @Bean
    @ConditionalOnBean(JmsTemplate.class)
    @ConditionalOnMissingBean
    public ActiveMQProducer activeMQProducer(JmsTemplate jmsTemplate) {
        return new ActiveMQProducer(jmsTemplate);
    }

    @Bean
    @ConditionalOnBean(JmsTemplate.class)
    @ConditionalOnMissingBean
    public ActiveMQConsumer activeMQConsumer(JmsTemplate jmsTemplate) {
        return new ActiveMQConsumer(jmsTemplate);
    }

    @Bean
    @ConditionalOnBean(RabbitTemplate.class)
    @ConditionalOnMissingBean
    public RabbitMQProducer rabbitMQProducer(RabbitTemplate rabbitTemplate) {
        return new RabbitMQProducer(rabbitTemplate);
    }

    @Bean
    @ConditionalOnBean(RabbitTemplate.class)
    @ConditionalOnMissingBean
    public RabbitMQConsumer rabbitMQConsumer(RabbitTemplate rabbitTemplate) {
        return new RabbitMQConsumer(rabbitTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public MQFactory mqFactory(@Nullable RocketMQProducer rocketMQProducer,
                               @Nullable RocketMQConsumer rocketMQConsumer,
                               StringRedisTemplate redisTemplate,
                               RedisMessageListenerContainer redisListenerContainer,
                               @Nullable ActiveMQProducer activeMQProducer,
                               @Nullable ActiveMQConsumer activeMQConsumer,
                               @Nullable RabbitMQProducer rabbitMQProducer,
                               @Nullable RabbitMQConsumer rabbitMQConsumer,
                               @Nullable EMQXProducer emqxProducer,
                               @Nullable EMQXConsumer emqxConsumer) {
        return new MQFactory(rocketMQProducer, rocketMQConsumer, redisTemplate, redisListenerContainer, 
                           activeMQProducer, activeMQConsumer, rabbitMQProducer, rabbitMQConsumer,
                           emqxProducer, emqxConsumer);
    }
    
    @Bean
    @ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
    @ConditionalOnMissingBean
    public RocketMQAdapter rocketMQAdapter(MQConfig mqConfig) throws Exception {
        MQConfig.RocketMQProperties rocketMQProps = mqConfig.getRocketmq();
        return new RocketMQAdapter(rocketMQProps.getProducerGroup(), rocketMQProps.getNameServerAddr());
    }
    
    @Bean
    @ConditionalOnBean(JmsTemplate.class)
    @ConditionalOnMissingBean
    public ActiveMQAdapter activeMQAdapter(JmsTemplate jmsTemplate) {
        return new ActiveMQAdapter(jmsTemplate);
    }
    
    @Bean
    @ConditionalOnBean(RabbitTemplate.class)
    @ConditionalOnMissingBean
    public RabbitMQAdapter rabbitMQAdapter(RabbitTemplate rabbitTemplate) {
        return new RabbitMQAdapter(rabbitTemplate);
    }
    
    @Bean
    @ConditionalOnBean(KafkaProducer.class)
    @ConditionalOnMissingBean
    public KafkaAdapter kafkaAdapter(KafkaProducer<String, byte[]> kafkaProducer) {
        return new KafkaAdapter(kafkaProducer);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public DelayMessageSender delayMessageSender(StringRedisTemplate redisTemplate, 
                                               List<MQAdapter> mqAdapters,
                                               MQConfig mqConfig) {
        return new DelayMessageSender(redisTemplate, mqAdapters, mqConfig);
    }
}