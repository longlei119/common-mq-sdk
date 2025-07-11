package com.example.mq.consumer.impl;

import com.example.mq.consumer.MQConsumer;
import com.example.mq.enums.MQTypeEnum;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * EMQX消息消费者
 */
@Component
public class EMQXConsumer implements MQConsumer, MqttCallback {
    
    private static final Logger logger = LoggerFactory.getLogger(EMQXConsumer.class);
    
    private final MqttClient mqttClient;
    private final ConcurrentHashMap<String, Consumer<String>> topicHandlers = new ConcurrentHashMap<>();
    
    public EMQXConsumer(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
        this.mqttClient.setCallback(this);
    }
    
    /**
     * 订阅主题并设置消息处理器
     */
    public void subscribe(String topic, Consumer<String> messageHandler) {
        try {
            topicHandlers.put(topic, messageHandler);
            mqttClient.subscribe(topic, 1); // QoS = 1
            logger.info("成功订阅EMQX主题: {}", topic);
        } catch (Exception e) {
            logger.error("订阅EMQX主题失败: {}", topic, e);
            throw new RuntimeException("订阅EMQX主题失败", e);
        }
    }
    
    /**
     * 取消订阅主题
     */
    public void unsubscribe(String topic) {
        try {
            topicHandlers.remove(topic);
            mqttClient.unsubscribe(topic);
            logger.info("成功取消订阅EMQX主题: {}", topic);
        } catch (Exception e) {
            logger.error("取消订阅EMQX主题失败: {}", topic, e);
            throw new RuntimeException("取消订阅EMQX主题失败", e);
        }
    }
    
    @Override
    public void connectionLost(Throwable cause) {
        logger.error("EMQX连接丢失", cause);
        // 可以在这里实现重连逻辑
    }
    
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String messageContent = new String(message.getPayload());
        logger.info("收到EMQX消息 - Topic: {}, Message: {}", topic, messageContent);
        
        Consumer<String> handler = topicHandlers.get(topic);
        if (handler != null) {
            try {
                handler.accept(messageContent);
            } catch (Exception e) {
                logger.error("处理EMQX消息失败 - Topic: {}, Message: {}", topic, messageContent, e);
            }
        } else {
            logger.warn("未找到主题 {} 的消息处理器", topic);
        }
    }
    
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        logger.debug("EMQX消息投递完成: {}", token.getMessageId());
    }
    
    // 实现MQConsumer接口方法
    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.EMQX) {
            throw new IllegalArgumentException("MQ type mismatch: expected EMQX, got " + mqType);
        }
        subscribe(topic, handler);
    }
    
    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.EMQX) {
            throw new IllegalArgumentException("MQ type mismatch: expected EMQX, got " + mqType);
        }
        unsubscribe(topic);
    }
    
    @Override
    public void start() {
        logger.info("EMQX消费者启动");
        // MQTT客户端在连接时已经启动，这里可以做一些初始化工作
    }
    
    @Override
    public void stop() {
        try {
            if (mqttClient.isConnected()) {
                mqttClient.disconnect();
                logger.info("EMQX消费者停止");
            }
        } catch (Exception e) {
            logger.error("停止EMQX消费者失败", e);
        }
    }
    
    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.EMQX;
    }
}