package com.example.mq.config;

import com.example.mq.consumer.impl.RocketMQConsumer;
import com.example.mq.factory.MQFactory;
import com.example.mq.producer.impl.RocketMQProducer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
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
import org.springframework.lang.Nullable;

@Configuration
@EnableConfigurationProperties(MQConfig.class)
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
    @ConditionalOnMissingBean
    public MQFactory mqFactory(@Nullable RocketMQProducer rocketMQProducer,
                              @Nullable RocketMQConsumer rocketMQConsumer,
                              StringRedisTemplate redisTemplate,
                              RedisMessageListenerContainer redisListenerContainer) {
        return new MQFactory(rocketMQProducer, rocketMQConsumer, redisTemplate, redisListenerContainer);
    }
}