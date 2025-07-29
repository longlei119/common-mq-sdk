# 死信队列设计与实现文档

## 1. 概述

死信队列（Dead Letter Queue，DLQ）是消息中间件中的一种特殊队列，用于存储无法被正常消费的消息。当消息在正常队列中多次消费失败后，会被转移到死信队列中，以便后续进行人工干预、重试或者分析问题。

本模块实现了一个通用的死信队列功能，支持多种MQ类型（RocketMQ、RabbitMQ、Kafka、ActiveMQ等），并提供了两种存储方式（Redis和MySQL）。

## 2. 功能特性

- **多种MQ支持**：兼容RocketMQ、RabbitMQ、Kafka、ActiveMQ等多种MQ类型
- **多种存储方式**：支持Redis和MySQL两种存储方式
- **自动重试机制**：支持配置重试次数、重试间隔和重试间隔倍数
- **自动清理机制**：支持配置自动清理过期消息
- **重试历史记录**：记录消息重试历史，包括重试时间、是否成功等信息
- **灵活配置**：支持通过配置文件灵活配置死信队列的各项参数

## 3. 架构设计

### 3.1 核心组件

- **DeadLetterMessage**：死信消息模型，包含消息ID、原始消息ID、主题、标签、消息体、属性、MQ类型、失败原因、重试次数、状态等信息
- **DeadLetterStatusEnum**：死信消息状态枚举，包含PENDING（待处理）、REDELIVERED（已重新投递）、DELIVERED（已投递成功）、FAILED（投递失败）
- **DeadLetterService**：死信队列服务接口，定义了死信队列的基本操作
- **AbstractDeadLetterService**：抽象实现类，提供了通用的死信队列操作逻辑
- **RedisDeadLetterService**：基于Redis的死信队列实现
- **MySQLDeadLetterService**：基于MySQL的死信队列实现
- **DeadLetterServiceFactory**：死信队列服务工厂，根据配置创建对应的死信队列服务实例
- **RetryHistory**：重试历史记录模型，记录消息重试的历史信息

### 3.2 状态流转

死信消息的状态流转如下：

```
PENDING（待处理）-> REDELIVERED（已重新投递）-> DELIVERED（已投递成功）/FAILED（投递失败）
```

## 4. 配置说明

在`application.yml`或`application-mq.yml`中添加以下配置：

```yaml
mq:
  dead-letter:
    enabled: true  # 是否启用死信队列
    storage-type: redis  # 存储类型，支持redis和mysql
    redis:
      key-prefix: dead_letter_  # Redis键前缀
      queue-key: dead_letter_queue  # 队列键名
      message-expiration: 604800000  # 消息过期时间，单位毫秒，默认7天
    mysql:
      auto-create-table: true  # 是否自动创建表
      message-table: t_dead_letter_message  # 消息表名
      history-table: t_dead_letter_retry_history  # 重试历史表名
      datasource-name: dataSource  # 数据源名称
    retry:
      max-retries: 3  # 最大重试次数
      retry-interval: 60000  # 初始重试间隔，单位毫秒
      retry-multiplier: 2.0  # 重试间隔倍数
    cleanup:
      auto-cleanup: true  # 是否自动清理
      cleanup-interval: 86400000  # 清理间隔，单位毫秒，默认1天
      message-retention: 604800000  # 消息保留时间，单位毫秒，默认7天
    scan-interval: 60000  # 扫描间隔，单位毫秒，默认1分钟
    batch-size: 100  # 批处理大小
```

## 5. 使用方法

### 5.1 自动处理

当消息消费失败并达到最大重试次数时，系统会自动将消息发送到死信队列。这个过程是在`MQConsumerAnnotationProcessor`中实现的，无需额外代码。

### 5.2 手动操作死信队列

可以通过注入`DeadLetterService`来手动操作死信队列：

```java
@Autowired
private DeadLetterServiceFactory deadLetterServiceFactory;

public void handleDeadLetterMessage() {
    DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
    if (deadLetterService == null) {
        // 死信队列未启用
        return;
    }
    
    // 获取死信消息列表
    List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
    
    // 重新投递消息
    for (DeadLetterMessage message : messages) {
        deadLetterService.redeliverMessage(message.getId());
    }
    
    // 获取重试历史
    List<RetryHistory> retryHistories = deadLetterService.getRetryHistory("messageId");
    
    // 删除死信消息
    deadLetterService.deleteDeadLetterMessage("messageId");
}
```

## 6. 数据库表结构（MySQL存储方式）

### 6.1 死信消息表

```sql
CREATE TABLE IF NOT EXISTS t_dead_letter_message (
    id VARCHAR(64) PRIMARY KEY,
    original_message_id VARCHAR(64),
    topic VARCHAR(255) NOT NULL,
    tag VARCHAR(255),
    body TEXT,
    properties TEXT,
    mq_type VARCHAR(50) NOT NULL,
    failure_reason TEXT,
    retry_count INT DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    create_timestamp BIGINT NOT NULL,
    update_timestamp BIGINT NOT NULL,
    INDEX idx_status (status),
    INDEX idx_create_timestamp (create_timestamp)
);
```

### 6.2 重试历史表

```sql
CREATE TABLE IF NOT EXISTS t_dead_letter_retry_history (
    id VARCHAR(64) PRIMARY KEY,
    message_id VARCHAR(64) NOT NULL,
    retry_timestamp BIGINT NOT NULL,
    success BOOLEAN NOT NULL,
    failure_reason TEXT,
    retry_count INT NOT NULL,
    INDEX idx_message_id (message_id)
);
```

## 7. 注意事项

1. 使用MySQL存储方式时，需要确保应用中已配置了JdbcTemplate和相应的数据源
2. 死信队列的消息默认保留7天，可以通过配置修改保留时间
3. 重试间隔采用指数退避算法，即每次重试的间隔会按照配置的倍数增加
4. 死信队列的自动清理功能默认启用，会定期清理过期的消息

## 8. 扩展点

如果需要支持其他存储方式，可以通过以下步骤扩展：

1. 实现`DeadLetterService`接口或继承`AbstractDeadLetterService`类
2. 在`DeadLetterServiceFactory`中添加对应的创建逻辑
3. 在配置中添加对应的存储类型和配置项

## 9. 性能考虑

- Redis存储方式适合高并发场景，但不适合存储大量数据
- MySQL存储方式适合存储大量数据，但性能较Redis略低
- 批量操作可以提高处理效率，可以通过配置`batch-size`参数调整批处理大小