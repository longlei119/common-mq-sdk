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

    public RedisConsumer(RedisMessageListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
        this.listenerMap = new HashMap<>();
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

        String channel = topic + ":" + tag;
        
        // 先移除已存在的监听器
        unsubscribe(mqType, topic, tag);
        
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
        listenerMap.put(channel, adapter);
        
        // 确保容器正在运行
        if (!listenerContainer.isRunning()) {
            listenerContainer.start();
        }
        
        log.info("Redis消费者订阅成功 - channel: {}", channel);
    }

    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        if (mqType != MQTypeEnum.REDIS) {
            return;
        }

        String channel = topic + ":" + tag;
        MessageListenerAdapter adapter = listenerMap.remove(channel);
        if (adapter != null) {
            listenerContainer.removeMessageListener(adapter);
            log.info("Redis消费者取消订阅成功 - channel: {}", channel);
        }
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
    }
}