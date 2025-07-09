package com.example.mq.producer;

import com.example.mq.enums.MQTypeEnum;
import com.example.mq.model.MQEvent;

/**
 * MQ生产者接口
 */
public interface MQProducer {

    /**
     * 同步发送消息
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     * @param event  事件
     * @return messageId 消息ID
     */
    String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event);

    /**
     * 异步发送消息
     *
     * @param mqType MQ类型
     * @param topic  主题
     * @param tag    标签
     * @param event  事件或字符串消息
     */
    void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event);

    /**
     * 异步发送延迟消息
     *
     * @param mqType      MQ类型
     * @param topic       主题
     * @param tag         标签
     * @param event       事件
     * @param delaySecond 延迟时间（秒）
     */
    void asyncSendDelay(MQTypeEnum mqType, String topic, String tag, MQEvent event, int delaySecond);
}