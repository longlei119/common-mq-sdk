# ActiveMQ适配器实现和消息中间件特性对比

## 1. ActiveMQ适配器实现

```java
public class ActiveMQAdapter implements MessageAdapter {
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Map<String, MessageConsumer> consumers;
    private Map<String, MessageProducer> producers;
    
    @Override
    public void init(AdapterConfig config) {
        // 初始化连接工厂
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
            config.getUsername(),
            config.getPassword(),
            String.format("tcp://%s:%d", config.getHost(), config.getPort())
        );
        
        // 配置连接池
        PooledConnectionFactory pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
        pooledFactory.setMaxConnections(config.getMaxConnections());
        pooledFactory.setMaximumActiveSessionPerConnection(config.getMaxSessionsPerConnection());
        
        this.connectionFactory = pooledFactory;
        this.consumers = new ConcurrentHashMap<>();
        this.producers = new ConcurrentHashMap<>();
    }
    
    @Override
    public void send(String topic, byte[] payload) {
        MessageProducer producer = getOrCreateProducer(topic);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(payload);
        producer.send(message);
    }
    
    @Override
    public void receive(String topic, MessageHandler handler) {
        MessageConsumer consumer = getOrCreateConsumer(topic);
        consumer.setMessageListener(message -> {
            if (message instanceof BytesMessage) {
                try {
                    BytesMessage bytesMessage = (BytesMessage) message;
                    byte[] payload = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(payload);
                    handler.handle(convertMessage(payload));
                } catch (Exception e) {
                    handler.handleError(null, e);
                }
            }
        });
    }
    
    private MessageProducer getOrCreateProducer(String topic) {
        return producers.computeIfAbsent(topic, t -> {
            try {
                Destination destination = session.createTopic(t);
                return session.createProducer(destination);
            } catch (JMSException e) {
                throw new MessageAdapterException("Failed to create producer", e);
            }
        });
    }
    
    private MessageConsumer getOrCreateConsumer(String topic) {
        return consumers.computeIfAbsent(topic, t -> {
            try {
                Destination destination = session.createTopic(t);
                return session.createConsumer(destination);
            } catch (JMSException e) {
                throw new MessageAdapterException("Failed to create consumer", e);
            }
        });
    }
    
    @Override
    public void close() {
        try {
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            throw new MessageAdapterException("Failed to close connection", e);
        }
    }
}
```

## 2. 消息中间件特性对比

### 2.1 功能特性对比

| 特性 | Redis | RocketMQ | ActiveMQ | EMQX |
|------|--------|-----------|-----------|------|
| 消息模型 | 发布/订阅 | 发布/订阅、点对点 | 发布/订阅、点对点 | 发布/订阅 |
| 消息持久化 | 可选 | 支持 | 支持 | 可选 |
| 消息优先级 | 不支持 | 支持 | 支持 | 不支持 |
| 消息过滤 | 模式匹配 | SQL92、Tag | 选择器 | Topic通配符 |
| 消息顺序 | 单通道有序 | 分区有序 | 队列有序 | 主题有序 |
| 事务消息 | 支持 | 支持 | 支持 | 不支持 |
| 延迟消息 | 不支持 | 支持 | 支持 | 不支持 |
| 死信队列 | 不支持 | 支持 | 支持 | 不支持 |
| 消息重试 | 不支持 | 支持 | 支持 | 不支持 |

### 2.2 性能特性对比

| 特性 | Redis | RocketMQ | ActiveMQ | EMQX |
|------|--------|-----------|-----------|------|
| 单机吞吐量 | 10万+/秒 | 10万+/秒 | 1-5万/秒 | 100万+/秒 |
| 消息延迟 | 微秒级 | 毫秒级 | 毫秒级 | 亚毫秒级 |
| 集群扩展性 | 高 | 高 | 中 | 高 |
| 消息堆积能力 | 受内存限制 | TB级 | GB级 | 受内存限制 |

### 2.3 应用场景对比

| 场景 | Redis | RocketMQ | ActiveMQ | EMQX |
|------|--------|-----------|-----------|------|
| 实时消息 | ✓✓✓ | ✓✓ | ✓✓ | ✓✓✓ |
| 可靠消息 | ✓ | ✓✓✓ | ✓✓ | ✓ |
| 海量消息 | ✓✓ | ✓✓✓ | ✓ | ✓✓✓ |
| 物联网场景 | ✓ | ✓✓ | ✓ | ✓✓✓ |
| 金融交易 | ✓ | ✓✓✓ | ✓✓ | ✓ |

### 2.4 运维特性对比

| 特性 | Redis | RocketMQ | ActiveMQ | EMQX |
|------|--------|-----------|-----------|------|
| 部署复杂度 | 低 | 中 | 低 | 中 |
| 监控能力 | 中 | 强 | 中 | 强 |
| 运维成本 | 低 | 中 | 低 | 中 |
| 社区活跃度 | 高 | 高 | 中 | 中 |
| 商业支持 | 有 | 有 | 有 | 有 |