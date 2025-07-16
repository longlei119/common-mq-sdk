package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.delay.adapter.RedisAdapter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@SpringBootTest
public class RedisAdapterTest {

    @Autowired(required = false)
    private RedisAdapter redisAdapter;
    
    @Autowired(required = false)
    private List<com.lachesis.windrangerms.mq.delay.adapter.MQAdapter> mqAdapters;

    @Test
    @ConditionalOnProperty(name = "mq.redis.enabled", havingValue = "true")
    void testRedisAdapterNotNull() {
        log.info("RedisAdapter: {}", redisAdapter);
        log.info("MQAdapters count: {}", mqAdapters != null ? mqAdapters.size() : 0);
        if (mqAdapters != null) {
            for (com.lachesis.windrangerms.mq.delay.adapter.MQAdapter adapter : mqAdapters) {
                log.info("MQAdapter: {} - {}", adapter.getClass().getSimpleName(), adapter.getMQType());
            }
        }
        assertNotNull(redisAdapter, "RedisAdapter should not be null");
    }
}