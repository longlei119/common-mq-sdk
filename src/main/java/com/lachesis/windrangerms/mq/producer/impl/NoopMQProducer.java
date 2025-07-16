package com.lachesis.windrangerms.mq.producer.impl;

import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;

/**
 * 空实现 MQProducer，未配置 MQ 时抛出异常
 */
public class NoopMQProducer implements MQProducer {
    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        throw new UnsupportedOperationException("未配置可用的 MQProducer，无法发送消息");
    }
    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        throw new UnsupportedOperationException("未配置可用的 MQProducer，无法发送消息");
    }
    @Override
    public String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        throw new UnsupportedOperationException("未配置可用的 MQProducer，无法发送延迟消息");
    }
    @Override
    public String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        throw new UnsupportedOperationException("未配置可用的 MQProducer，无法发送广播消息");
    }
    @Override
    public void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event) {
        throw new UnsupportedOperationException("未配置可用的 MQProducer，无法发送广播消息");
    }
    @Override
    public String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        throw new UnsupportedOperationException("未配置可用的 MQProducer，无法发送延迟广播消息");
    }
    @Override
    public MQTypeEnum getMQType() { return null; }
}