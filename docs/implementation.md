# 统一消息中间件SDK实现细节

## 1. 核心实现

### 1.1 消息模型

```java
public class Message<T> {
    private String id;          // 消息唯一标识
    private String topic;       // 消息主题
    private T payload;         // 消息内容
    private Map<String, Object> headers;  // 消息头
    private long timestamp;    // 时间戳
    private String source;     // 消息来源
    private MessageType type;  // 消息类型
}

public enum MessageType {
    NORMAL,     // 普通消息
    BROADCAST,  // 广播消息
    DELAY,      // 延迟消息
    RETRY,      // 重试消息
    DEAD_LETTER // 死信消息
}
```

### 1.2 适配器实现

#### Redis适配器
```java
public class RedisMessageAdapter extends AbstractMessageAdapter {
    private StringRedisTemplate redisTemplate;
    private RedisMessageListenerContainer listenerContainer;
    
    @Override
    public void publish(String topic, Object message) {
        try {
            String messageStr = messageConverter.convertToString(message);
            redisTemplate.convertAndSend(topic, messageStr);
        } catch (Exception e) {
            handlePublishError(topic, message, e);
        }
    }
    
    @Override
    public void subscribe(String topic, MessageHandler handler) {
        MessageListener listener = (message, pattern) -> {
            try {
                Object payload = messageConverter.convertFromString(message.toString());
                Message<?> msg = createMessage(topic, payload);
                handler.onMessage(msg);
            } catch (Exception e) {
                handler.onError(null, e);
            }
        };
        listenerContainer.addMessageListener(listener, new ChannelTopic(topic));
    }
}
```

#### RocketMQ适配器
```java
public class RocketMQAdapter extends AbstractMessageAdapter {
    private DefaultMQProducer producer;
    private DefaultMQPushConsumer consumer;
    
    @Override
    public void publish(String topic, Object message) {
        try {
            byte[] messageBytes = messageConverter.convertToBytes(message);
            Message msg = new Message(topic, messageBytes);
            producer.send(msg);
        } catch (Exception e) {
            handlePublishError(topic, message, e);
        }
    }
    
    @Override
    public void subscribe(String topic, MessageHandler handler) {
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                try {
                    Object payload = messageConverter.convertFromBytes(msg.getBody());
                    Message<?> message = createMessage(topic, payload);
                    handler.onMessage(message);
                } catch (Exception e) {
                    handler.onError(null, e);
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }
}
```

### 1.3 消息转换器

```java
public interface MessageConverter {
    String convertToString(Object message);
    Object convertFromString(String message);
    byte[] convertToBytes(Object message);
    Object convertFromBytes(byte[] message);
}

public class JsonMessageConverter implements MessageConverter {
    private ObjectMapper objectMapper;
    
    @Override
    public String convertToString(Object message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert message to string", e);
        }
    }
    
    @Override
    public Object convertFromString(String message) {
        try {
            return objectMapper.readValue(message, Object.class);
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert string to message", e);
        }
    }
}
```

### 1.4 重试机制

```java
public class RetryTemplate {
    private int maxAttempts;
    private long initialInterval;
    private double multiplier;
    private long maxInterval;
    
    public <T> T execute(RetryCallback<T> retryCallback) {
        int attempts = 0;
        long interval = initialInterval;
        
        while (attempts < maxAttempts) {
            try {
                return retryCallback.doWithRetry();
            } catch (Exception e) {
                attempts++;
                if (attempts >= maxAttempts) {
                    throw new MaxRetriesExceededException(e);
                }
                sleep(interval);
                interval = Math.min((long)(interval * multiplier), maxInterval);
            }
        }
        throw new MaxRetriesExceededException();
    }
}
```

### 1.5 监控实现

```java
public class MessageMetrics {
    private Counter publishedMessages;
    private Counter consumedMessages;
    private Counter failedMessages;
    private Timer messageProcessingTime;
    private Gauge queueSize;
    
    public void recordPublish() {
        publishedMessages.increment();
    }
    
    public void recordConsume() {
        consumedMessages.increment();
    }
    
    public void recordFailure() {
        failedMessages.increment();
    }
    
    public Timer.Sample startMessageProcessing() {
        return Timer.start();
    }
    
    public void stopMessageProcessing(Timer.Sample sample) {
        sample.stop(messageProcessingTime);
    }
}
```

## 2. 高级特性实现

### 2.1 消息幂等性

```java
public class IdempotencyHandler {
    private RedisTemplate<String, String> redisTemplate;
    private static final String IDEMPOTENCY_KEY_PREFIX = "msg:idempotency:";
    
    public boolean isProcessed(String messageId) {
        String key = IDEMPOTENCY_KEY_PREFIX + messageId;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }
    
    public void markAsProcessed(String messageId, Duration expiration) {
        String key = IDEMPOTENCY_KEY_PREFIX + messageId;
        redisTemplate.opsForValue().set(key, "1", expiration);
    }
}
```

### 2.2 消息路由

```java
public class MessageRouter {
    private Map<String, List<MessageHandler>> routingTable;
    
    public void route(Message<?> message) {
        String topic = message.getTopic();
        List<MessageHandler> handlers = routingTable.get(topic);
        if (handlers != null) {
            for (MessageHandler handler : handlers) {
                try {
                    handler.onMessage(message);
                } catch (Exception e) {
                    handler.onError(message, e);
                }
            }
        }
    }
}
```

### 2.3 消息过滤

```java
public interface MessageFilter {
    boolean accept(Message<?> message);
}

public class CompositeMessageFilter implements MessageFilter {
    private List<MessageFilter> filters;
    
    @Override
    public boolean accept(Message<?> message) {
        return filters.stream().allMatch(filter -> filter.accept(message));
    }
}
```

## 3. 性能优化

### 3.1 连接池配置

```java
public class ConnectionPoolConfig {
    private int maxTotal = 8;
    private int maxIdle = 8;
    private int minIdle = 0;
    private long maxWaitMillis = -1L;
    private boolean testOnBorrow = false;
    private boolean testOnReturn = false;
    private boolean testWhileIdle = true;
    private long timeBetweenEvictionRunsMillis = 60000L;
    
    public GenericObjectPool<?> createPool(PooledObjectFactory<?> factory) {
        GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        config.setMaxWaitMillis(maxWaitMillis);
        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnReturn(testOnReturn);
        config.setTestWhileIdle(testWhileIdle);
        config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        
        return new GenericObjectPool<>(factory, config);
    }
}
```

### 3.2 批量处理

```java
public class BatchMessageHandler implements MessageHandler {
    private int batchSize;
    private Duration maxWaitTime;
    private List<Message<?>> messageBuffer;
    private ScheduledExecutorService scheduler;
    
    public void onMessage(Message<?> message) {
        messageBuffer.add(message);
        if (messageBuffer.size() >= batchSize) {
            processBatch();
        }
    }
    
    private void processBatch() {
        List<Message<?>> batch = new ArrayList<>(messageBuffer);
        messageBuffer.clear();
        // 处理批量消息
        processBatchMessages(batch);
    }
}
```

## 4. 测试策略

### 4.1 单元测试

```java
@SpringBootTest
public class MessageAdapterTest {
    @Autowired
    private MessagePublisher publisher;
    
    @Autowired
    private MessageSubscriber subscriber;
    
    @Test
    public void testPublishAndSubscribe() {
        String topic = "test-topic";
        String message = "test-message";
        CountDownLatch latch = new CountDownLatch(1);
        
        subscriber.subscribe(topic, new MessageHandler() {
            @Override
            public void onMessage(Message<?> msg) {
                assertEquals(message, msg.getPayload());
                latch.countDown();
            }
        });
        
        publisher.publish(topic, message);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
}
```

### 4.2 集成测试

```java
@SpringBootTest
@TestPropertySource(properties = {
    "message.redis.enabled=true",
    "message.redis.host=localhost"
})
public class MessageIntegrationTest {
    @Autowired
    private MessagePublisher publisher;
    
    @Autowired
    private MessageSubscriber subscriber;
    
    @Test
    public void testMessageDelivery() {
        // 测试消息投递的可靠性
    }
    
    @Test
    public void testMessageRetry() {
        // 测试消息重试机制
    }
    
    @Test
    public void testCircuitBreaker() {
        // 测试熔断器功能
    }
}
```