package com.lachesis.windrangerms.mq.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import com.lachesis.windrangerms.mq.config.DeadLetterAutoConfiguration;

/**
 * MQ自动配置测试
 */
@SpringBootTest(classes = {MQAutoConfiguration.class, DeadLetterAutoConfiguration.class})
@TestPropertySource(properties = {
    "mq.dead-letter.enabled=true",
    "mq.dead-letter.storage-type=redis",
    "spring.redis.host=localhost",
    "spring.redis.port=6379"
})
public class MQAutoConfigurationTest {

    @Test
    public void contextLoads() {
        // 测试应用上下文能够正常加载
        // 如果配置有问题，这个测试会失败
    }
}