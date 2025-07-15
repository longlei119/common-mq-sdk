# MQ SDK 注解驱动使用指南

## 概述

本MQ SDK现已支持注解驱动的消息消费模式，通过`@MQConsumer`注解可以轻松定义消息消费者，无需手动管理消费者的订阅和取消订阅。

## 核心特性

- **注解驱动**：使用`@MQConsumer`注解声明式定义消息消费者
- **统一抽象**：支持RocketMQ、RabbitMQ、ActiveMQ、EMQX、Redis等多种MQ
- **模式选择**：支持单播(UNICAST)和广播(BROADCAST)两种消息模式
- **自动管理**：自动扫描、注册、启动和停止消费者
- **简化配置**：消息模式由消费者端决定，生产者无需区分

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>common-mq-sdk</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. 配置MQ连接

```yaml
mq:
  enabled: true
  rocketmq:
    name-server-addr: localhost:9876
    producer-group: default-producer-group
    consumer-group: default-consumer-group
  rabbitmq:
    enabled: true
    host: localhost
    port: 5672
    username: guest
    password: guest
  redis:
    host: localhost
    port: 6379
```

### 3. 定义消息消费者

```java
@Service
public class MessageService {
    
    // 单播消息消费者
    @MQConsumer(
        mqType = MQType.ROCKETMQ,
        topic = "user-topic",
        tag = "register",
        mode = MessageMode.UNICAST,
        consumerGroup = "user-service-group"
    )
    public void handleUserRegister(String message) {
        log.info("处理用户注册消息: {}", message);
        // 业务逻辑处理
    }
    
    // 广播消息消费者
    @MQConsumer(
        mqType = MQType.ROCKETMQ,
        topic = "system-topic",
        tag = "notification",
        mode = MessageMode.BROADCAST,
        consumerGroup = "notification-service-group"
    )
    public void handleSystemNotification(String message) {
        log.info("处理系统通知广播消息: {}", message);
        // 业务逻辑处理
    }
}
```

### 4. 发送消息

```java
@Service
public class MessageProducerService {
    
    @Autowired
    private MQFactory mqFactory;
    
    public void sendUserRegisterMessage(String userId) {
        MQProducer producer = mqFactory.getProducer(MQType.ROCKETMQ);
        producer.syncSend("user-topic", "register", userId);
    }
    
    public void sendSystemNotification(String notification) {
        MQProducer producer = mqFactory.getProducer(MQType.ROCKETMQ);
        producer.syncSend("system-topic", "notification", notification);
    }
}
```

## 注解参数说明

### @MQConsumer

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| mqType | MQType | 是 | - | MQ类型(ROCKETMQ, RABBITMQ, ACTIVEMQ, EMQX, REDIS) |
| topic | String | 是 | - | 主题名称 |
| tag | String | 否 | "" | 标签，用于消息过滤 |
| mode | MessageMode | 否 | UNICAST | 消息模式(UNICAST单播, BROADCAST广播) |
| consumerGroup | String | 否 | "" | 消费者组名 |

## 消息模式说明

### 单播模式 (UNICAST)
- 消息只会被一个消费者实例接收
- 适用于任务处理、订单处理等场景
- 保证消息不会重复处理

### 广播模式 (BROADCAST)
- 消息会被所有订阅的消费者实例接收
- 适用于配置更新、系统通知等场景
- 每个消费者实例都会收到消息

## 不同MQ的实现差异

### RocketMQ
- **单播**：使用CLUSTERING模式，消息在消费者组内负载均衡
- **广播**：使用BROADCASTING模式，每个消费者实例都收到消息

### RabbitMQ
- **单播**：使用DirectExchange，消息路由到队列，消费者竞争消费
- **广播**：使用FanoutExchange，消息广播到所有绑定的队列

### ActiveMQ
- **单播**：使用Queue模式，消息点对点传递
- **广播**：使用Topic模式，消息发布订阅

### EMQX/Redis
- **单播**：通过消费者组实现负载均衡
- **广播**：每个消费者使用独立的订阅

## 最佳实践

1. **方法签名**：消费者方法必须是public，且只能有一个String类型参数
2. **异常处理**：在消费者方法中进行适当的异常处理
3. **幂等性**：确保消费者方法具有幂等性，避免重复处理问题
4. **性能考虑**：避免在消费者方法中执行耗时操作，考虑异步处理
5. **监控日志**：添加适当的日志记录，便于问题排查

## 注意事项

1. 消费者方法必须在Spring管理的Bean中
2. 确保MQ服务已启动并配置正确
3. 广播模式下，每个应用实例都会收到消息
4. 消费者组名建议使用有意义的名称，便于管理
5. 标签可以为空，表示接收该主题下的所有消息

## 迁移指南

如果您正在从旧版本的MQ SDK迁移，请注意：

1. 生产者接口已简化，移除了广播相关方法
2. 消费者现在通过注解定义，无需手动订阅
3. 消息模式由消费者端决定，生产者只负责发送
4. 自动配置会处理消费者的启动和停止

通过这种注解驱动的方式，您可以更加简洁和声明式地定义消息消费者，提高开发效率和代码可维护性。