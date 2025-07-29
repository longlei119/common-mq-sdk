# MessageAckResult 使用指南

## 概述

为了提供更精确的消息确认控制，MQ SDK 现在支持消费者方法返回 `MessageAckResult` 枚举来明确指示消息处理结果。这个新功能完全向后兼容现有的 `void` 方法。

## MessageAckResult 枚举值

| 枚举值 | 描述 | 行为 |
|--------|------|------|
| `SUCCESS` | 消息处理成功 | 确认消息（ACK），清除重试计数 |
| `RETRY` | 需要重试 | 增加重试计数，达到最大重试次数后进入死信队列 |
| `REJECT` | 拒绝消息 | 直接将消息发送到死信队列（如果启用） |
| `IGNORE` | 忽略消息 | 确认消息但不做任何处理，清除重试计数 |

## 使用方法

### 1. 新的返回值方式（推荐）

```java
@MQConsumer(
    mqType = MQTypeEnum.RABBITMQ,
    topic = "example.topic",
    tag = "example.tag",
    consumerGroup = "example-group"
)
public MessageAckResult handleMessage(String message) {
    try {
        // 业务逻辑处理
        if (processMessage(message)) {
            return MessageAckResult.SUCCESS;
        } else {
            return MessageAckResult.RETRY;
        }
    } catch (ValidationException e) {
        // 数据格式错误，不需要重试
        return MessageAckResult.REJECT;
    } catch (Exception e) {
        // 其他异常，需要重试
        return MessageAckResult.RETRY;
    }
}
```

### 2. 传统的 void 方法（向后兼容）

```java
@MQConsumer(
    mqType = MQTypeEnum.RABBITMQ,
    topic = "legacy.topic",
    tag = "legacy.tag",
    consumerGroup = "legacy-group"
)
public void handleLegacyMessage(String message) {
    try {
        // 业务逻辑处理
        processMessage(message);
        // 正常执行完成，自动 ACK
    } catch (Exception e) {
        // 抛出异常触发重试
        throw new RuntimeException("处理失败", e);
    }
}
```

## 使用场景

### 1. 成功处理
```java
public MessageAckResult handleSuccessMessage(String message) {
    // 处理业务逻辑
    saveToDatabase(message);
    return MessageAckResult.SUCCESS;
}
```

### 2. 需要重试
```java
public MessageAckResult handleRetryMessage(String message) {
    try {
        callExternalService(message);
        return MessageAckResult.SUCCESS;
    } catch (ServiceUnavailableException e) {
        // 外部服务不可用，稍后重试
        return MessageAckResult.RETRY;
    }
}
```

### 3. 拒绝消息
```java
public MessageAckResult handleRejectMessage(String message) {
    if (!isValidFormat(message)) {
        // 消息格式无效，直接拒绝
        return MessageAckResult.REJECT;
    }
    // 处理有效消息
    return MessageAckResult.SUCCESS;
}
```

### 4. 忽略消息
```java
public MessageAckResult handleIgnoreMessage(String message) {
    if (isDuplicateMessage(message)) {
        // 重复消息，忽略处理
        return MessageAckResult.IGNORE;
    }
    // 处理新消息
    return MessageAckResult.SUCCESS;
}
```

## 异常处理

### 返回值方法的异常处理

当使用 `MessageAckResult` 返回值的方法抛出异常时，系统会自动将其视为 `MessageAckResult.RETRY`：

```java
public MessageAckResult handleMessageWithException(String message) {
    // 如果这里抛出异常，系统会自动按 RETRY 处理
    processMessage(message);
    return MessageAckResult.SUCCESS;
}
```

### void 方法的异常处理

void 方法保持原有的异常处理逻辑：
- 无异常：自动 ACK
- 有异常：触发重试，达到最大重试次数后进入死信队列

## 迁移指南

### 从 void 方法迁移到 MessageAckResult

**迁移前：**
```java
public void handleMessage(String message) {
    try {
        processMessage(message);
    } catch (BusinessException e) {
        // 业务异常，不重试
        log.error("业务处理失败", e);
        // 只能通过不抛出异常来 ACK
    } catch (SystemException e) {
        // 系统异常，需要重试
        throw e;
    }
}
```

**迁移后：**
```java
public MessageAckResult handleMessage(String message) {
    try {
        processMessage(message);
        return MessageAckResult.SUCCESS;
    } catch (BusinessException e) {
        // 业务异常，拒绝消息
        log.error("业务处理失败", e);
        return MessageAckResult.REJECT;
    } catch (SystemException e) {
        // 系统异常，需要重试
        log.error("系统异常", e);
        return MessageAckResult.RETRY;
    }
}
```

## 最佳实践

1. **明确的返回值**：优先使用 `MessageAckResult` 返回值，提供更清晰的语义

2. **异常分类**：区分业务异常和系统异常，选择合适的 ACK 策略

3. **日志记录**：为不同的 ACK 结果添加适当的日志

4. **渐进式迁移**：可以逐步将现有的 void 方法迁移到新的返回值方式

5. **测试覆盖**：确保所有 ACK 场景都有相应的测试用例

## 注意事项

1. **向后兼容**：现有的 void 方法无需修改，继续正常工作

2. **方法签名验证**：消费者方法的返回类型必须是 `void` 或 `MessageAckResult`

3. **死信队列**：`REJECT` 和达到最大重试次数的 `RETRY` 都需要启用死信队列配置

4. **性能考虑**：新的返回值处理不会影响现有性能

## 配置要求

确保在配置文件中启用死信队列（如果需要使用 REJECT 功能）：

```yaml
windrangerms:
  mq:
    dead-letter:
      enabled: true
      retry:
        max-retries: 3
```