# 统一消息队列SDK

## 项目介绍
这是一个统一的消息队列SDK，旨在为不同的消息队列系统提供统一的接口，简化消息的发布和订阅操作。

### 主要特性
1. 统一接口：对上层提供一致的消息订阅/发布API
2. 中间件隔离：底层实现与业务代码解耦
3. 自动适配：根据配置动态选择中间件实现
4. 扩展性：新增中间件只需添加新的适配器实现
5. 支持多种消息队列：Redis、RocketMQ、Kafka、RabbitMQ、ActiveMQ等
6. 延迟消息：支持精确的延迟投递和消息重试机制

## 项目结构
```
src/main/java/com/example/mq/
├── consumer        // 消费者接口
├── producer        // 生产者接口
├── model           // 消息模型
├── enums           // 枚举定义
├── delay           // 延迟消息功能
│   ├── adapter     // 消息队列适配器
│   ├── model       // 延迟消息模型
│   └── DelayMessageSender  // 延迟消息发送器
└── example         // 使用示例
```

## 使用方法

### 1. 引入依赖
```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>common-mq-sdk</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

### 2. 消息发送示例
```java
// 注入MQProducer
@Autowired
private MQProducer mqProducer;

// 构建消息事件
OrderSyncEvent event = OrderSyncEvent.builder()
        .orderId("1001")
        .orderNum("ORDER_123")
        .data(orderInfo)
        .build();

// 发送消息
mqProducer.syncSend(MQTypeEnum.ROCKET_MQ, event.getTopic(), event.getTag(), event);
```

### 3. 消息订阅示例
```java
@Autowired
private MQConsumer mqConsumer;

@PostConstruct
public void init() {
    // 订阅消息
    mqConsumer.subscribe(MQTypeEnum.ROCKET_MQ, "mall", "order_sync", message -> {
        OrderSyncEvent event = JSON.parseObject(message, OrderSyncEvent.class);
        // 处理消息
        handleOrderSync(event);
    });
    
    // 启动消费者
    mqConsumer.start();
}
```

## 延迟消息功能

### 配置方法
在`application.yml`中添加相关配置：

```yaml
mq:
  delay:
    enabled: true
    redis-key-prefix: "delay_message:"
    scan-interval: 1000
    batch-size: 100
    message-expire-time: 604800000  # 7天，单位毫秒
    default-mq-type: "ROCKET_MQ"
    retry:
      max-retries: 3
      retry-interval: 5000
      retry-multiplier: 2
```

### 使用方法

```java
// 注入DelayMessageSender
@Autowired
private DelayMessageSender delayMessageSender;

// 方式一：直接发送DelayMessage对象
DelayMessage message = new DelayMessage();
message.setId(UUID.randomUUID().toString());
message.setTopic("your-topic");
message.setTag("your-tag");
message.setBody("消息内容");
message.setMqTypeEnum(MQTypeEnum.ROCKET_MQ); // 可选值：ROCKET_MQ, ACTIVE_MQ, RABBIT_MQ, KAFKA
message.setDeliverTimestamp(System.currentTimeMillis() + 60000); // 60秒后投递

Map<String, String> properties = new HashMap<>();
properties.put("key1", "value1");
message.setProperties(properties);

delayMessageSender.sendDelayMessage(message);

// 方式二：指定主题、标签、内容和延迟时间
delayMessageSender.sendDelayMessage("your-topic", "your-tag", "消息内容", "ROCKET_MQ", 60000);
```

### 适配器扩展
如需支持新的消息队列，只需实现`MQAdapter`接口：

```java
public class NewMQAdapter implements MQAdapter {
    @Override
    public void send(DelayMessage message) {
        // 实现消息发送逻辑
    }
    
    @Override
    public String getMQType() {
        return "NEW_MQ";
    }
}
```

然后在Spring配置中注册该适配器：

```java
@Bean
public NewMQAdapter newMQAdapter() {
    return new NewMQAdapter();
}
```

## 注意事项
1. 使用前需要确保相应的消息队列中间件已经正确安装和配置
2. 根据实际使用的消息队列类型引入相应的依赖
3. 建议在项目启动时初始化消费者，并在项目关闭时停止消费者
4. 延迟消息功能依赖Redis，请确保Redis配置正确
5. 延迟消息的最大延迟时间受Redis过期时间限制，默认为7天
6. 消息重试机制使用指数退避策略，可通过配置调整重试次数和间隔

## 版本要求
- JDK 1.8+
- Spring Boot 2.3.12.RELEASE+