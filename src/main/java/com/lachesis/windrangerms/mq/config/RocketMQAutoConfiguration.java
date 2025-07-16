package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.impl.RocketMQConsumer;
import com.lachesis.windrangerms.mq.producer.impl.RocketMQProducer;
import com.lachesis.windrangerms.mq.delay.adapter.RocketMQAdapter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnClass(name = "org.apache.rocketmq.client.consumer.DefaultMQPushConsumer")
@Configuration
public class RocketMQAutoConfiguration {
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
    @ConditionalOnProperty(prefix = "mq.rocketmq", name = {"enabled", "name-server-addr"}, havingValue = "true")
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
    @ConditionalOnProperty(prefix = "mq.rocketmq", name = "name-server-addr")
    @ConditionalOnMissingBean
    public RocketMQAdapter rocketMQAdapter(MQConfig mqConfig) throws Exception {
        MQConfig.RocketMQProperties rocketMQProps = mqConfig.getRocketmq();
        return new RocketMQAdapter(rocketMQProps.getProducerGroup(), rocketMQProps.getNameServerAddr());
    }
}