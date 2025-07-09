# 统一消息中间件SDK设计文档

## 1. 系统概述

### 1.1 设计目标
- 提供统一的消息订阅/发布接口
- 实现中间件与业务代码的解耦
- 支持多种消息中间件的动态适配
- 保证系统的可扩展性
- 确保消息处理的可靠性和性能

### 1.2 支持的消息中间件
- Redis Pub/Sub
- RocketMQ
- Kafka
- RabbitMQ
- EMQX

## 2. 系统架构

### 2.1 消息中间件特性对比
的是
<table>
  <tr>
    <th style="background-color: #f0f0f0; padding: 8px;">特性</th>
    <th style="background-color: #f0f0f0; padding: 8px;">Redis</th>
    <th style="background-color: #f0f0f0; padding: 8px;">RocketMQ</th>
    <th style="background-color: #f0f0f0; padding: 8px;">ActiveMQ</th>
    <th style="background-color: #f0f0f0; padding: 8px;">RabbitMQ</th>
    <th style="background-color: #f0f0f0; padding: 8px;">EMQX</th>
  </tr>
  <tr>
    <td colspan="6" style="background-color: #e6e6e6; font-weight: bold;">消息模型</td>
  </tr>
  <tr>
    <td>点对点队列</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>发布订阅</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>主题分区</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>消息过滤</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td colspan="6" style="background-color: #e6e6e6; font-weight: bold;">消息特性</td>
  </tr>
  <tr>
    <td>消息确认机制</td>
    <td>发布确认</td>
    <td>多级确认</td>
    <td>JMS确认</td>
    <td>确认模式</td>
    <td>QoS级别</td>
  </tr>
  <tr>
    <td>延迟消息</td>
    <td>Zset实现</td>
    <td>18个级别</td>
    <td>调度器</td>
    <td>插件支持</td>
    <td>不支持</td>
  </tr>
  <tr>
    <td>事务消息</td>
    <td>MULTI/EXEC</td>
    <td>分布式事务</td>
    <td>XA事务</td>
    <td>分布式事务</td>
    <td>不支持</td>
  </tr>
  <tr>
    <td>顺序消息</td>
    <td>单队列FIFO</td>
    <td>全局/分区顺序</td>
    <td>队列顺序</td>
    <td>队列顺序</td>
    <td>主题顺序</td>
  </tr>
  <tr>
    <td>消息优先级</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✗</td>
  </tr>
  <tr>
    <td>消息存活时间</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td colspan="6" style="background-color: #e6e6e6; font-weight: bold;">可靠性</td>
  </tr>
  <tr>
    <td>持久化机制</td>
    <td>RDB/AOF</td>
    <td>文件存储</td>
    <td>文件/数据库</td>
    <td>文件存储</td>
    <td>外部存储</td>
  </tr>
  <tr>
    <td>副本机制</td>
    <td>主从</td>
    <td>主从</td>
    <td>主从</td>
    <td>镜像队列</td>
    <td>集群</td>
  </tr>
  <tr>
    <td>故障恢复</td>
    <td>主从切换</td>
    <td>故障切换</td>
    <td>故障切换</td>
    <td>自动切换</td>
    <td>集群容错</td>
  </tr>
  <tr>
    <td>消息重试</td>
    <td>应用层</td>
    <td>内置重试</td>
    <td>内置重试</td>
    <td>内置重试</td>
    <td>会话重试</td>
  </tr>
  <tr>
    <td>死信处理</td>
    <td>不支持</td>
    <td>死信队列</td>
    <td>DLQ</td>
    <td>死信交换机</td>
    <td>不支持</td>
  </tr>
  <tr>
    <td colspan="6" style="background-color: #e6e6e6; font-weight: bold;">扩展性</td>
  </tr>
  <tr>
    <td>集群模式</td>
    <td>分片集群</td>
    <td>主从集群</td>
    <td>网络集群</td>
    <td>镜像集群</td>
    <td>分布式集群</td>
  </tr>
  <tr>
    <td>动态扩容</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>跨中心部署</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>协议支持</td>
    <td>RESP</td>
    <td>自定义</td>
    <td>OpenWire/STOMP</td>
    <td>AMQP</td>
    <td>MQTT</td>
  </tr>
  <tr>
    <td>插件机制</td>
    <td>模块</td>
    <td>插件</td>
    <td>插件</td>
    <td>插件</td>
    <td>插件</td>
  </tr>
  <tr>
    <td colspan="6" style="background-color: #e6e6e6; font-weight: bold;">性能</td>
  </tr>
  <tr>
    <td>单机吞吐量</td>
    <td>10w+/s</td>
    <td>10w+/s</td>
    <td>1w+/s</td>
    <td>5w+/s</td>
    <td>10w+/s</td>
  </tr>
  <tr>
    <td>消息延迟</td>
    <td>微秒级</td>
    <td>毫秒级</td>
    <td>毫秒级</td>
    <td>微秒级</td>
    <td>毫秒级</td>
  </tr>
  <tr>
    <td>并发连接数</td>
    <td>10w+</td>
    <td>10w+</td>
    <td>1w+</td>
    <td>10w+</td>
    <td>100w+</td>
  </tr>
  <tr>
    <td>消息堆积</td>
    <td>内存限制</td>
    <td>磁盘限制</td>
    <td>磁盘限制</td>
    <td>内存限制</td>
    <td>磁盘限制</td>
  </tr>
  <tr>
    <td colspan="6" style="background-color: #e6e6e6; font-weight: bold;">运维特性</td>
  </tr>
  <tr>
    <td>监控工具</td>
    <td>Redis-cli</td>
    <td>控制台</td>
    <td>Web控制台</td>
    <td>Web管理界面</td>
    <td>Dashboard</td>
  </tr>
  <tr>
    <td>多租户</td>
    <td>命名空间</td>
    <td>命名空间</td>
    <td>虚拟主机</td>
    <td>虚拟主机</td>
    <td>命名空间</td>
  </tr>
  <tr>
    <td>安全机制</td>
    <td>密码认证</td>
    <td>ACL</td>
    <td>JAAS</td>
    <td>RBAC</td>
    <td>ACL</td>
  </tr>
  <tr>
    <td>运维难度</td>
    <td>简单</td>
    <td>中等</td>
    <td>中等</td>
    <td>中等</td>
    <td>中等</td>
  </tr>
  <tr>
    <td colspan="6" style="background-color: #e6e6e6; font-weight: bold;">应用场景</td>
  </tr>
  <tr>
    <td>实时消息</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>延迟队列</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✗</td>
  </tr>
  <tr>
    <td>事件驱动</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>流式处理</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✓</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✓</td>
  </tr>
  <tr>
    <td>物联网</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✗</td>
    <td style="text-align: center;">✓</td>
  </tr>
</table>

### 2.2 核心组件

#### 2.2.1 统一接口层
- MessagePublisher：消息发布接口
- MessageSubscriber：消息订阅接口
- MessageHandler：消息处理接口

#### 2.2.2 适配器层
- AbstractMessageAdapter：抽象适配器基类
- RedisMessageAdapter：Redis实现
- RocketMQAdapter：RocketMQ实现
- KafkaAdapter：Kafka实现
- RabbitMQAdapter：RabbitMQ实现
- EMQXAdapter：EMQX实现

#### 2.2.3 配置管理
- MessageProperties：消息配置属性类
- MessageAutoConfiguration：自动配置类

#### 2.2.4 公共组件
- MessageConverter：消息转换器
- RetryTemplate：重试模板
- CircuitBreaker：熔断器

### 2.3 架构特点
- 采用适配器模式实现中间件适配
- 使用工厂模式创建适配器实例
- 通过策略模式处理不同类型的消息
- 实现观察者模式进行消息通知