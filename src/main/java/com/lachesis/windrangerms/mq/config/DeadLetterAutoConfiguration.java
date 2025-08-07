package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.deadletter.MySQLDeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.RedisDeadLetterService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 死信队列自动配置类
 */
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "mq.dead-letter", name = "enabled", havingValue = "true")
public class DeadLetterAutoConfiguration {
    
    public DeadLetterAutoConfiguration() {
        log.info("DeadLetterAutoConfiguration 正在初始化...");
    }

    /**
     * 配置Redis实现的死信队列服务
     */
    @Bean
    @ConditionalOnProperty(prefix = "mq.dead-letter", name = "storage-type", havingValue = "redis", matchIfMissing = true)
    public RedisDeadLetterService redisDeadLetterService() {
        log.info("正在创建 RedisDeadLetterService bean");
        return new RedisDeadLetterService();
    }

    /**
     * 配置MySQL实现的死信队列服务
     */
    @Bean
    @ConditionalOnProperty(prefix = "mq.dead-letter", name = "storage-type", havingValue = "mysql")
    @ConditionalOnMissingBean
    public MySQLDeadLetterService mysqlDeadLetterService() {
        log.info("正在创建 MySQLDeadLetterService bean");
        return new MySQLDeadLetterService();
    }

    /**
     * 配置死信队列服务工厂
     */
    @Bean
    @ConditionalOnMissingBean
    public DeadLetterServiceFactory deadLetterServiceFactory() {
        log.info("正在创建 DeadLetterServiceFactory bean");
        return new DeadLetterServiceFactory();
    }
}