package com.lachesis.windrangerms.mq.consumer.impl;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import java.util.function.Consumer;

/**
 * 空实现 MQConsumer，未配置 MQ 时抛出异常
 */
public class NoopMQConsumer implements MQConsumer {
    @Override
    public void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法订阅消息");
    }
    @Override
    public void subscribeUnicast(String topic, String tag, Consumer<String> handler, String consumerGroup) {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法订阅单播消息");
    }
    @Override
    public void subscribeBroadcast(String topic, String tag, Consumer<String> handler) {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法订阅广播消息");
    }
    @Override
    public void subscribeBroadcast(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler) {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法订阅广播消息");
    }
    @Override
    public void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag) {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法取消广播订阅");
    }
    @Override
    public void unsubscribe(MQTypeEnum mqType, String topic, String tag) {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法取消订阅");
    }
    @Override
    public void start() {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法启动");
    }
    @Override
    public void stop() {
        throw new UnsupportedOperationException("未配置可用的 MQConsumer，无法停止");
    }
    @Override
    public MQTypeEnum getMQType() { return null; }
}