# Kafka服务器配置修复指南

## 问题描述

测试失败的根本原因是Kafka服务器配置问题，而非客户端配置问题。客户端正确连接到`10.2.3.161:9092`，但Kafka服务器在元数据响应中返回的broker地址是`localhost:9092`，导致客户端无法建立后续连接。

## 错误日志分析

```
Connection to node 1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
Topic test-kafka-deadletter-topic not present in metadata after 60000 ms.
```

## 解决方案

### 1. 修改Kafka服务器配置

在Kafka服务器的`server.properties`文件中添加或修改以下配置：

```properties
# 设置Kafka服务器的监听地址
listeners=PLAINTEXT://0.0.0.0:9092

# 设置客户端连接时使用的地址（关键配置）
advertised.listeners=PLAINTEXT://10.2.3.161:9092

# 可选：设置broker ID
broker.id=1
```

### 2. 如果使用Docker部署

在`docker-compose.yml`中添加环境变量：

```yaml
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://10.2.3.161:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
```

### 3. 重启Kafka服务

修改配置后，重启Kafka服务使配置生效：

```bash
# 如果是直接部署
sudo systemctl restart kafka

# 如果是Docker部署
docker-compose restart kafka
```

## 配置说明

- `listeners`: Kafka服务器监听的地址，`0.0.0.0:9092`表示监听所有网络接口
- `advertised.listeners`: **关键配置**，告诉客户端应该使用哪个地址连接broker，这个地址会包含在Kafka返回的元数据中
- 必须将`advertised.listeners`设置为客户端可以访问的实际IP地址`10.2.3.161:9092`

## 验证修复

配置修改并重启服务后，重新运行测试：

```bash
mvn test -Dtest=KafkaDeadLetterTest#testKafkaMessageFailureToDeadLetter
```

测试应该能够成功连接并完成。

## 注意事项

1. 确保防火墙允许9092端口的访问
2. 如果Kafka部署在容器中，确保网络配置正确
3. `advertised.listeners`必须使用客户端能够解析和访问的地址
4. 客户端配置（application-mq.yml）已经正确，无需修改