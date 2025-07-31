package com.lachesis.windrangerms.mq.deadletter;

import com.lachesis.windrangerms.mq.config.MQConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 死信队列服务工厂
 * 根据配置创建合适的死信队列服务实现
 */
@Slf4j
@Component
public class DeadLetterServiceFactory {

    @Autowired
    private MQConfig mqConfig;
    
    @Autowired(required = false)
    private RedisDeadLetterService redisDeadLetterService;
    
    @Autowired(required = false)
    private MySQLDeadLetterService mysqlDeadLetterService;
    
    /**
     * 获取死信队列服务实现
     * 根据配置的存储类型返回对应的实现
     * 
     * @return 死信队列服务实现
     */
    public DeadLetterService getDeadLetterService() {
        if (!mqConfig.getDeadLetter().isEnabled()) {
            log.warn("Dead letter queue is disabled");
            return null;
        }
        
        String storageType = mqConfig.getDeadLetter().getStorageType().toLowerCase();
        
        switch (storageType) {
            case "redis":
                if (redisDeadLetterService == null) {
                    log.warn("Redis dead letter service is not available");
                    return null;
                }
                log.debug("Using Redis dead letter service");
                return redisDeadLetterService;
            case "mysql":
                if (mysqlDeadLetterService == null) {
                    log.warn("MySQL dead letter service is not available");
                    return null;
                }
                log.debug("Using MySQL dead letter service");
                return mysqlDeadLetterService;
            default:
                log.warn("Unknown storage type: {}, fallback to Redis", storageType);
                if (redisDeadLetterService == null) {
                    log.warn("Redis dead letter service is not available for fallback, no dead letter service available");
                    return null;
                }
                return redisDeadLetterService;
        }
    }
}