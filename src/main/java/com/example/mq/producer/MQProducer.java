package com.example.mq.producer;

import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;

/**
 * MQ生产者接口
 * 注意：广播模式现在由消费者端的@MQConsumer注解决定，生产者只负责发送消息
 */
public interface MQProducer {

    /**
     * 同步发送消息
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     * @param event  消息事件
     * @return 消息ID
     */
    String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event);

    /**
     * 异步发送消息
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     * @param event  消息事件
     */
    void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event);

    /**
     * 异步发送延迟消息
     *
     * @param mqType      MQ类型
     * @param topic       主题
     * @param tag         标签
     * @param body        消息体
     * @param delaySecond 延迟时间（秒）
     * @return 消息ID
     */
    String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond);

    /**
     * 同步发送广播消息
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     * @param event  消息事件
     * @return 消息ID
     */
    String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event);

    /**
     * 异步发送广播消息
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     * @param event  消息事件
     */
    void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event);

    /**
     * 异步发送延迟广播消息
     *
     * @param mqType      MQ类型
     * @param topic       主题
     * @param tag         标签
     * @param body        消息体
     * @param delaySecond 延迟时间（秒）
     * @return 消息ID
     */
    String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond);

    /**
     * 获取MQ类型
     *
     * @return MQ类型
     */
    MQTypeEnum getMQType();
}