package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.consumer.impl.RedisConsumer;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.delay.adapter.RedisAdapter;
import com.lachesis.windrangerms.mq.producer.impl.RedisProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Redis消息队列自动配置类
 */
@Configuration
@ConditionalOnClass(name = "org.springframework.data.redis.core.StringRedisTemplate")
public class RedisAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public RedisConnectionFactory redisConnectionFactory(MQConfig mqConfig) {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(mqConfig.getRedis().getHost());
        config.setPort(mqConfig.getRedis().getPort());
        config.setDatabase(mqConfig.getRedis().getDatabase());
        return new LettuceConnectionFactory(config);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        return new StringRedisTemplate(redisConnectionFactory);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory redisConnectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        return container;
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public RedisAdapter redisAdapter(StringRedisTemplate redisTemplate) {
        return new RedisAdapter(redisTemplate);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public RedisConsumer redisConsumer(RedisMessageListenerContainer listenerContainer) {
        return new RedisConsumer(listenerContainer);
    }

    @Bean
    @ConditionalOnProperty(prefix = "mq.redis", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public RedisProducer redisProducer(StringRedisTemplate redisTemplate, 
                                       @Autowired(required = false) DelayMessageSender delayMessageSender) {
        return new RedisProducer(redisTemplate, delayMessageSender);
    }
}