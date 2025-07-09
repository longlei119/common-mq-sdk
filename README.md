# 统一消息队列SDK

## 项目介绍
这是一个统一的消息队列SDK，旨在为不同的消息队列系统提供统一的接口，简化消息的发布和订阅操作。

### 主要特性
1. 统一接口：对上层提供一致的消息订阅/发布API
2. 中间件隔离：底层实现与业务代码解耦
3. 自动适配：根据配置动态选择中间件实现
4. 扩展性：新增中间件只需添加新的适配器实现
5. 支持多种消息队列：Redis、RocketMQ、Kafka、RabbitMQ、ActiveMQ等

## 项目结构
```
src/main/java/com/example/mq/
├── consumer        // 消费者接口
├── producer        // 生产者接口
├── model           // 消息模型
├── enums           // 枚举定义
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

## 注意事项
1. 使用前需要确保相应的消息队列中间件已经正确安装和配置
2. 根据实际使用的消息队列类型引入相应的依赖
3. 建议在项目启动时初始化消费者，并在项目关闭时停止消费者

## 版本要求
- JDK 1.8+
- Spring Boot 2.3.12.RELEASE+