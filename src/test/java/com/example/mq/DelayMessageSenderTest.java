package com.example.mq;

import com.example.mq.delay.DelayMessageSender;
import com.example.mq.delay.adapter.MQAdapter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@SpringBootTest
public class DelayMessageSenderTest {

    @Autowired
    private ApplicationContext applicationContext;
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;
    
    @Autowired(required = false)
    private StringRedisTemplate stringRedisTemplate;
    
    @Autowired(required = false)
    private List<MQAdapter> mqAdapters;

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testDelayMessageSenderBeanCreation() {
        log.info("=== DelayMessageSender Bean Creation Test ===");
        log.info("StringRedisTemplate: {}", stringRedisTemplate != null ? "EXISTS" : "NULL");
        log.info("MQAdapters count: {}", mqAdapters != null ? mqAdapters.size() : 0);
        
        if (mqAdapters != null) {
            for (MQAdapter adapter : mqAdapters) {
                log.info("MQAdapter: {} - {}", adapter.getClass().getSimpleName(), adapter.getMQType());
            }
        }
        
        log.info("DelayMessageSender: {}", delayMessageSender != null ? "EXISTS" : "NULL");
        
        // 检查Bean是否在ApplicationContext中
        String[] beanNames = applicationContext.getBeanNamesForType(DelayMessageSender.class);
        log.info("DelayMessageSender beans in context: {}", java.util.Arrays.toString(beanNames));
        
        // 检查配置属性
        try {
            String delayEnabled = applicationContext.getEnvironment().getProperty("mq.delay.enabled");
            String redisHost = applicationContext.getEnvironment().getProperty("mq.redis.host");
            log.info("mq.delay.enabled: {}", delayEnabled);
            log.info("mq.redis.host: {}", redisHost);
        } catch (Exception e) {
            log.error("Error reading properties: {}", e.getMessage());
        }
        
        assertNotNull(delayMessageSender, "DelayMessageSender should not be null");
    }
}