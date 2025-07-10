package com.example.mq.delay.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;

/**
 * 延迟消息消费者示例
 * 展示如何消费通过延迟消息系统发送的消息
 */
@Component
public class DelayMessageConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(DelayMessageConsumerExample.class);

    @Value("${mq.rocketmq.name-server-addr}")
    private String nameServer;

    @Value("${mq.rocketmq.consumer-group}")
    private String consumerGroup;

    private DefaultMQPushConsumer consumer;

    /**
     * 初始化消费者
     */
    @PostConstruct
    public void init() {
        try {
            // 创建消费者
            consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(nameServer);
            
            // 订阅延迟消息主题
            consumer.subscribe("delay-message-topic", "*");
            
            // 注册消息监听器
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        try {
                            // 获取消息内容
                            String body = new String(msg.getBody(), "UTF-8");
                            String topic = msg.getTopic();
                            String tags = msg.getTags();
                            String keys = msg.getKeys();
                            
                            // 获取自定义属性
                            String customProperty = msg.getProperty("custom_property");
                            
                            // 处理消息
                            logger.info("收到延迟消息: topic={}, tags={}, keys={}, body={}, custom_property={}", 
                                    topic, tags, keys, body, customProperty);
                            
                            // 执行业务逻辑
                            processDelayMessage(body, topic, tags, keys, customProperty);
                            
                            // 返回消费成功状态
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        } catch (Exception e) {
                            // 记录异常
                            logger.error("处理延迟消息异常", e);
                            
                            // 返回消费失败状态，消息将重试
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            
            // 启动消费者
            consumer.start();
            logger.info("延迟消息消费者已启动，消费者组: {}, 名称服务器: {}", consumerGroup, nameServer);
        } catch (MQClientException e) {
            logger.error("启动延迟消息消费者失败", e);
        }
    }

    /**
     * 处理延迟消息的业务逻辑
     */
    private void processDelayMessage(String body, String topic, String tags, String keys, String customProperty) {
        // 这里实现具体的业务逻辑
        // 例如：处理订单超时、发送提醒通知等
        logger.info("正在处理延迟消息，业务逻辑执行中...");
    }

    /**
     * 销毁消费者
     */
    @PreDestroy
    public void destroy() {
        if (consumer != null) {
            consumer.shutdown();
            logger.info("延迟消息消费者已关闭");
        }
    }
}