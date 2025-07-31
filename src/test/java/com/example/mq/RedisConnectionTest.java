package com.example.mq;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

public class RedisConnectionTest {

    @Test
    public void testRedisConnection() {
        try {
            // 创建Redis连接配置
            RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
            config.setHostName("localhost");
            config.setPort(6379);
            config.setDatabase(0);
            
            // 创建连接工厂
            LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config);
            connectionFactory.afterPropertiesSet();
            
            // 创建StringRedisTemplate
            StringRedisTemplate redisTemplate = new StringRedisTemplate();
            redisTemplate.setConnectionFactory(connectionFactory);
            redisTemplate.afterPropertiesSet();
            
            // 测试连接
            String pong = redisTemplate.getConnectionFactory().getConnection().ping();
            System.out.println("Redis连接测试成功: " + pong);
            
            // 测试基本操作
            redisTemplate.opsForValue().set("test:key", "test:value");
            String value = redisTemplate.opsForValue().get("test:key");
            System.out.println("Redis读写测试成功: " + value);
            
            // 清理测试数据
            redisTemplate.delete("test:key");
            
            // 关闭连接
            connectionFactory.destroy();
            
        } catch (Exception e) {
            System.err.println("Redis连接失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}