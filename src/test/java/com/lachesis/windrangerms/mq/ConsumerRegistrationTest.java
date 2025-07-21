package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.consumer.MQConsumerManager;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试消费者注册情况
 */
@SpringBootTest
// 使用默认统一配置
public class ConsumerRegistrationTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerRegistrationTest.class);

    @Autowired
    private MQConsumerManager mqConsumerManager;

    @Test
    public void testConsumerRegistration() throws Exception {
        assertNotNull(mqConsumerManager, "MQConsumerManager should not be null");
        
        // 使用反射获取 consumerMap
        Field field = mqConsumerManager.getClass().getDeclaredField("consumerMap");
        field.setAccessible(true);
        Map<MQTypeEnum, ?> consumerMap = (Map<MQTypeEnum, ?>) field.get(mqConsumerManager);
        
        logger.info("已注册的消费者类型: {}", consumerMap.keySet());
        
        for (Map.Entry<MQTypeEnum, ?> entry : consumerMap.entrySet()) {
            logger.info("MQ类型: {}, 实现类: {}", entry.getKey(), entry.getValue().getClass().getSimpleName());
        }
        
        // 检查 RocketMQ 消费者是否注册
        assertTrue(consumerMap.containsKey(MQTypeEnum.ROCKET_MQ), "RocketMQ消费者应该已注册");
        
        logger.info("消费者注册测试通过");
    }
}