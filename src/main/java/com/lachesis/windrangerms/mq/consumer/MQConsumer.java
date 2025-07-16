package com.lachesis.windrangerms.mq.consumer;

import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import java.util.function.Consumer;

/**
 * MQ消费者接口
 */
public interface MQConsumer {

    /**
     * 订阅消息
     * 消费者只需要关心订阅什么内容，消息传递模式（单播/广播）由生产者决定
     *
     * @param mqType  MQ类型
     * @param topic   主题
     * @param tag     标签
     * @param handler 消息处理器
     */
    void subscribe(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler);
    
    /**
     * 订阅单播消息
     *
     * @param topic         主题
     * @param tag           标签
     * @param handler       消息处理器
     * @param consumerGroup 消费者组
     */
    void subscribeUnicast(String topic, String tag, Consumer<String> handler, String consumerGroup);
    
    /**
     * 订阅广播消息
     * @param topic 主题
     * @param tag 标签
     * @param handler 消息处理器
     */
    void subscribeBroadcast(String topic, String tag, Consumer<String> handler);

    /**
     * 订阅广播消息（带MQ类型）
     * @param mqType MQ类型
     * @param topic 主题
     * @param tag 标签
     * @param handler 消息处理器
     */
    void subscribeBroadcast(MQTypeEnum mqType, String topic, String tag, Consumer<String> handler);
    
    /**
     * 取消广播订阅
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     */
    void unsubscribeBroadcast(MQTypeEnum mqType, String topic, String tag);

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

    /**
     * 获取MQ类型
     *
     * @return MQ类型
     */
    MQTypeEnum getMQType();
}