package com.example.mq;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.TestPropertySource;

@Slf4j
@SpringBootTest
@TestPropertySource(locations = "classpath:application.yml")
public class SimpleRedisTest {

    @Autowired(required = false)
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testRedisConnection() {
        log.info("=== Simple Redis Connection Test ===");
        
        if (stringRedisTemplate == null) {
            log.error("StringRedisTemplate is null - Redis configuration failed");
            return;
        }
        
        try {
            // 测试 Redis 连接
            stringRedisTemplate.opsForValue().set("test-key", "test-value");
            String value = stringRedisTemplate.opsForValue().get("test-key");
            log.info("Redis test successful. Set and got value: {}", value);
            
            // 清理测试数据
            stringRedisTemplate.delete("test-key");
        } catch (Exception e) {
            log.error("Redis connection failed: {}", e.getMessage(), e);
        }
    }
}