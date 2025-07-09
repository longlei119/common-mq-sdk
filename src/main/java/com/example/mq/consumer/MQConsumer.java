package com.example.mq.consumer;

import com.example.mq.enums.MQTypeEnum;
import java.util.function.Consumer;

/**
 * MQ消费者接口
 */
public interface MQConsumer {

    /**
     * 订阅消息
     *
     * @param mqType  MQ类型
     * @param topic   主题
     * @param tag     标签
     * @param handler 消息处理器
     */
    void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler);

    /**
     * 取消订阅
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     */
    void unsubscribe(MQTypeEnum mqType, String topic, String tag);

    /**
     * 启动消费者
     */
    void start();

    /**
     * 停止消费者
     */
    void stop();
}