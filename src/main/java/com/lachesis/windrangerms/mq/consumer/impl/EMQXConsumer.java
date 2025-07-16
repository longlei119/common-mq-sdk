package com.lachesis.windrangerms.mq.consumer.impl;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * EMQX消息消费者
 */
@Component
@ConditionalOnBean(MqttClient.class)
public class EMQXConsumer implements MQConsumer, MqttCallback {
    
    private static final Logger logger = LoggerFactory.getLogger(EMQXConsumer.class);
    
    private final MqttClient mqttClient;
    private final ConcurrentHashMap<String, Consumer<String>> topicHandlers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Consumer<String>> broadcastHandlers = new ConcurrentHashMap<>();
    
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
        
        // 处理单播消息
        Consumer<String> handler = topicHandlers.get(topic);
        if (handler != null) {
            try {
                handler.accept(messageContent);
            } catch (Exception e) {
                logger.error("处理EMQX消息失败 - Topic: {}, Message: {}", topic, messageContent, e);
            }
            return;
        }
        
        // 处理广播消息
        if (topic.contains("/broadcast/")) {
            for (Consumer<String> broadcastHandler : broadcastHandlers.values()) {
                try {
                    broadcastHandler.accept(messageContent);
                    logger.debug("EMQX广播消息处理成功: topic={}, message={}", topic, messageContent);
                } catch (Exception e) {
                    logger.error("EMQX广播消息处理失败: topic={}, message={}, error={}", topic, messageContent, e.getMessage(), e);
                }
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
        
        try {
            // 同时订阅单播和广播消息
            subscribeUnicastInternal(topic, tag, handler);
            subscribeBroadcastInternal(topic, tag, handler);
            
            logger.info("EMQX统一订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            logger.error("EMQX订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("EMQX订阅失败", e);
        }
    }
    
    @Override
    public void subscribeUnicast(String topic, String tag, Consumer<String> handler, String consumerGroup) {
        subscribeUnicastInternal(topic, tag, handler);
    }

    @Override
    public void subscribeBroadcast(String topic, String tag, Consumer<String> handler) {
        subscribeBroadcastInternal(topic, tag, handler);
    }

    @Override
    public void subscribeBroadcast(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        if (mqType != MQTypeEnum.EMQX) {
            return;
        }
        subscribeBroadcastInternal(topic, tag, handler);
    }

    /**
     * 单播订阅的内部实现
     */
    private void subscribeUnicastInternal(String topic, String tag, Consumer<String> handler) {
        try {
            String fullTopic = tag != null ? topic + "/" + tag : topic;
            topicHandlers.put(fullTopic, handler);
            mqttClient.subscribe(fullTopic, 1); // QoS = 1
            logger.debug("EMQX单播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            logger.error("EMQX单播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("EMQX单播订阅失败", e);
        }
    }
    
    /**
     * 广播订阅的内部实现
     */
    private void subscribeBroadcastInternal(String topic, String tag, Consumer<String> handler) {
        try {
            // MQTT广播模式使用通配符主题，添加UUID确保每个消费者独立
            String broadcastTopic = topic + "/broadcast" + (tag != null ? "/" + tag : "") + "/+";
            String consumerKey = topic + ":" + (tag != null ? tag : "") + ":" + java.util.UUID.randomUUID().toString();
            
            broadcastHandlers.put(consumerKey, handler);
            mqttClient.subscribe(broadcastTopic, 1); // QoS = 1
            
            logger.info("EMQX广播订阅成功: topic={}, tag={}, consumerKey={}", topic, tag, consumerKey);
        } catch (Exception e) {
            logger.error("EMQX广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("EMQX广播订阅失败", e);
        }
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.EMQX) {
            throw new IllegalArgumentException("MQ type mismatch: expected EMQX, got " + mqType);
        }
        
        try {
            // 取消单播订阅
            unsubscribeUnicast(topic, tag);
            
            // 取消广播订阅
            unsubscribeBroadcastInternal(topic, tag);
            
            logger.info("EMQX取消订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            logger.error("EMQX取消订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("EMQX取消订阅失败", e);
        }
    }
    
    @Override
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.EMQX) {
            return;
        }
        
        try {
            unsubscribeBroadcastInternal(topic, tag);
            logger.info("EMQX取消广播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            logger.error("EMQX取消广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("EMQX取消广播订阅失败", e);
        }
    }
    
    /**
     * 取消单播订阅的内部实现
     */
    private void unsubscribeUnicast(String topic, String tag) {
        try {
            String fullTopic = tag != null ? topic + "/" + tag : topic;
            topicHandlers.remove(fullTopic);
            mqttClient.unsubscribe(fullTopic);
            logger.debug("EMQX取消单播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            logger.error("EMQX取消单播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("EMQX取消单播订阅失败", e);
        }
    }
    
    /**
     * 取消广播订阅的内部实现
     */
    private void unsubscribeBroadcastInternal(String topic, String tag) {
        try {
            String keyPrefix = topic + ":" + (tag != null ? tag : "") + ":";
            String broadcastTopic = topic + "/broadcast" + (tag != null ? "/" + tag : "") + "/+";
            
            // 移除所有匹配的广播订阅
            broadcastHandlers.entrySet().removeIf(entry -> {
                if (entry.getKey().startsWith(keyPrefix)) {
                    logger.info("EMQX取消广播订阅成功: consumerKey={}", entry.getKey());
                    return true;
                }
                return false;
            });
            
            // 取消MQTT主题订阅
            mqttClient.unsubscribe(broadcastTopic);
        } catch (Exception e) {
            logger.error("EMQX取消广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("EMQX取消广播订阅失败", e);
        }
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
            
            // 清理资源
            topicHandlers.clear();
            broadcastHandlers.clear();
            
            logger.info("EMQX消费者资源清理完成");
        } catch (Exception e) {
            logger.error("停止EMQX消费者失败", e);
        }
    }
    
    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.EMQX;
    }
}