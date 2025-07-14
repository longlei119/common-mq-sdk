package com.example.mq.consumer.impl;

import com.example.mq.consumer.MQConsumer;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class RedisConsumer implements MQConsumer {

    private final RedisMessageListenerContainer listenerContainer;
    private final Map<String, MessageListenerAdapter> listenerMap;
    private final Map<String, MessageListenerAdapter> broadcastListenerMap;
    // 用于跟踪每个channel的所有监听器，实现真正的单播
    private final Map<String, MessageListenerAdapter> channelListenerMap;

    public RedisConsumer(RedisMessageListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
        this.listenerMap = new HashMap<>();
        this.broadcastListenerMap = new HashMap<>();
        this.channelListenerMap = new HashMap<>();
        // 确保容器启动
        if (!listenerContainer.isRunning()) {
            listenerContainer.start();
        }
    }

    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> messageHandler) {
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }
        
        try {
            // 同时订阅单播和广播消息
            subscribeUnicast(topic, tag, messageHandler, "");
            subscribeBroadcast(topic, tag, messageHandler);
            
            log.info("Redis统一订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("Redis订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("Redis订阅失败", e);
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
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }
        subscribeBroadcastInternal(topic, tag, handler);
    }
    
    /**
     * 单播订阅的内部实现
     */
    private void subscribeUnicastInternal(String topic, String tag, Consumer<String> messageHandler) {
        String channel = topic + ":" + tag;
        
        // Redis单播模式：先移除该channel的所有现有监听器，确保只有一个消费者
        MessageListenerAdapter existingAdapter = channelListenerMap.get(channel);
        if (existingAdapter != null) {
            listenerContainer.removeMessageListener(existingAdapter);
            log.info("Redis移除已存在的单播监听器 - channel: {}", channel);
        }
        
        MessageListener listener = (message, pattern) -> {
            try {
                String messageBody = new String(message.getBody(), StandardCharsets.UTF_8);
                log.debug("Redis收到原始消息 - channel: {}, message: {}", channel, messageBody);
                messageHandler.accept(messageBody);
                log.info("Redis消息处理成功 - channel: {}, message: {}", channel, messageBody);
            } catch (Exception e) {
                log.error("Redis消息处理失败 - channel: {}, error: {}", channel, e.getMessage(), e);
            }
        };

        MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
        listenerContainer.addMessageListener(adapter, new ChannelTopic(channel));
        
        // 存储到channelListenerMap，确保每个channel只有一个监听器
        channelListenerMap.put(channel, adapter);
        
        // 确保容器正在运行
        if (!listenerContainer.isRunning()) {
            listenerContainer.start();
        }
        
        log.debug("Redis单播订阅成功: topic={}, tag={}", topic, tag);
    }

    /**
     * 广播订阅的内部实现
     */
    private void subscribeBroadcastInternal(String topic, String tag, Consumer<String> messageHandler) {
        // 广播模式使用特殊的channel命名，添加UUID确保每个消费者独立
        String broadcastChannel = topic + ".broadcast:" + (tag != null ? tag : "");
        String consumerKey = broadcastChannel + ":" + java.util.UUID.randomUUID().toString();
        
        MessageListener listener = (message, pattern) -> {
            try {
                String messageBody = new String(message.getBody(), StandardCharsets.UTF_8);
                log.debug("Redis收到广播消息 - channel: {}, message: {}", broadcastChannel, messageBody);
                messageHandler.accept(messageBody);
                log.info("Redis广播消息处理成功 - channel: {}, message: {}", broadcastChannel, messageBody);
            } catch (Exception e) {
                log.error("Redis广播消息处理失败 - channel: {}, error: {}", broadcastChannel, e.getMessage(), e);
            }
        };

        MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
        listenerContainer.addMessageListener(adapter, new ChannelTopic(broadcastChannel));
        broadcastListenerMap.put(consumerKey, adapter);
        
        // 确保容器正在运行
        if (!listenerContainer.isRunning()) {
            listenerContainer.start();
        }
        
        log.info("Redis广播订阅成功: topic={}, tag={}, consumerKey={}", topic, tag, consumerKey);
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }
        
        try {
            // 取消单播订阅
            unsubscribeUnicastInternal(topic, tag);
            
            // 取消广播订阅
            unsubscribeBroadcastInternal(topic, tag);
            
            log.info("Redis取消订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("Redis取消订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("Redis取消订阅失败", e);
        }
    }
    
    @Override
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }
        
        try {
            unsubscribeBroadcastInternal(topic, tag);
            log.info("Redis取消广播订阅成功: topic={}, tag={}", topic, tag);
        } catch (Exception e) {
            log.error("Redis取消广播订阅失败: topic={}, tag={}, error={}", topic, tag, e.getMessage(), e);
            throw new RuntimeException("Redis取消广播订阅失败", e);
        }
    }
    
    /**
     * 取消单播订阅的内部实现
     */
    private void unsubscribeUnicastInternal(String topic, String tag) {
        String channel = topic + ":" + tag;
        MessageListenerAdapter adapter = channelListenerMap.remove(channel);
        if (adapter != null) {
            listenerContainer.removeMessageListener(adapter);
            log.debug("Redis取消单播订阅成功: topic={}, tag={}", topic, tag);
        }
    }
    
    /**
     * 取消广播订阅的内部实现
     */
    private void unsubscribeBroadcastInternal(String topic, String tag) {
        String broadcastChannelPrefix = topic + ".broadcast:" + (tag != null ? tag : "") + ":";
        
        // 移除所有匹配的广播监听器
        broadcastListenerMap.entrySet().removeIf(entry -> {
            if (entry.getKey().startsWith(broadcastChannelPrefix)) {
                listenerContainer.removeMessageListener(entry.getValue());
                log.info("Redis取消广播订阅成功: consumerKey={}", entry.getKey());
                return true;
            }
            return false;
        });
    }

    @Override
    public void start() {
        if (!listenerContainer.isRunning()) {
            listenerContainer.start();
            log.info("Redis消费者启动成功");
        }
    }

    @Override
    public void stop() {
        if (listenerContainer.isRunning()) {
            listenerContainer.stop();
            log.info("Redis消费者停止成功");
        }
        
        // 清理资源
        listenerMap.clear();
        broadcastListenerMap.clear();
        channelListenerMap.clear();
        
        log.info("Redis消费者资源清理完成");
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.REDIS;
    }
}