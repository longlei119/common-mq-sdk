package com.lachesis.windrangerms.mq.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

@Configuration
@ConditionalOnProperty(prefix = "mq.redis", name = "host")
public class RedisConfiguration {

    @Bean
    public RedisConnectionFactory redisConnectionFactory(MQConfig mqConfig) {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(mqConfig.getRedis().getHost());
        config.setPort(mqConfig.getRedis().getPort());
        config.setDatabase(mqConfig.getRedis().getDatabase());
        return new LettuceConnectionFactory(config);
    }
}