package com.lachesis.windrangerms.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/**
 * EMQX消息生产者
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
public class EMQXProducer implements MQProducer {
    
    private final MqttClient mqttClient;
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;
    
    public EMQXProducer(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
    }
    
    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.EMQX) {
            return null;
        }
        try {
            String message = JSON.toJSONString(event);
            log.info("EMQX同步发送消息：topic={}, tag={}, message={}", topic, tag, message);
            
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setPayload(message.getBytes());
            mqttMessage.setQos(1); // 设置QoS为1，确保消息至少被传递一次
            mqttMessage.setRetained(false);
            
            // MQTT的topic格式：topic/tag
            String mqttTopic = tag != null ? topic + "/" + tag : topic;
            mqttClient.publish(mqttTopic, mqttMessage);
            
            // 生成消息ID
            String messageId = UUID.randomUUID().toString();
            log.info("EMQX同步发送消息成功：topic={}, tag={}, messageId={}", topic, tag, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("EMQX同步发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
            return null;
        }
    }
    
    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.EMQX) {
            return;
        }
        try {
            String message = event instanceof String ? (String) event : JSON.toJSONString(event);
            log.info("EMQX异步发送消息：topic={}, tag={}, message={}", topic, tag, message);
            
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setPayload(message.getBytes());
            mqttMessage.setQos(1);
            mqttMessage.setRetained(false);
            
            // MQTT的topic格式：topic/tag
            String mqttTopic = tag != null ? topic + "/" + tag : topic;
            
            // MQTT发送本身就是异步的
            mqttClient.publish(mqttTopic, mqttMessage);
            
            String messageId = UUID.randomUUID().toString();
            log.info("EMQX异步发送消息成功：topic={}, tag={}, messageId={}", topic, tag, messageId);
        } catch (Exception e) {
            log.error("EMQX异步发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
        }
    }
    
    @Override
    public String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.EMQX) {
            return null;
        }
        // MQTT协议本身不支持延迟消息，通过DelayMessageSender实现
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        return delayMessageSender.sendDelayMessage(topic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        if (mqType != MQTypeEnum.EMQX) {
            return null;
        }
        try {
            String message = JSON.toJSONString(event);
            log.info("EMQX同步广播发送消息：topic={}, tag={}, message={}", topic, tag, message);
            
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setPayload(message.getBytes());
            mqttMessage.setQos(1); // 设置QoS为1，确保消息至少被传递一次
            mqttMessage.setRetained(false);
            
            // MQTT广播模式的topic格式：topic/broadcast/tag/uuid
            String broadcastTopic = topic + "/broadcast" + (tag != null ? "/" + tag : "") + "/" + UUID.randomUUID().toString();
            mqttClient.publish(broadcastTopic, mqttMessage);
            
            // 生成消息ID
            String messageId = UUID.randomUUID().toString();
            log.info("EMQX同步广播发送消息成功：topic={}, tag={}, messageId={}", topic, tag, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("EMQX同步广播发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
            return null;
        }
    }

    @Override
    public void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event) {
        if (mqType != MQTypeEnum.EMQX) {
            return;
        }
        try {
            String message = event instanceof String ? (String) event : JSON.toJSONString(event);
            log.info("EMQX异步广播发送消息：topic={}, tag={}, message={}", topic, tag, message);
            
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setPayload(message.getBytes());
            mqttMessage.setQos(1);
            mqttMessage.setRetained(false);
            
            // MQTT广播模式的topic格式：topic/broadcast/tag/uuid
            String broadcastTopic = topic + "/broadcast" + (tag != null ? "/" + tag : "") + "/" + UUID.randomUUID().toString();
            
            // MQTT发送本身就是异步的
            mqttClient.publish(broadcastTopic, mqttMessage);
            
            String messageId = UUID.randomUUID().toString();
            log.info("EMQX异步广播发送消息成功：topic={}, tag={}, messageId={}", topic, tag, messageId);
        } catch (Exception e) {
            log.error("EMQX异步广播发送消息失败：topic={}, tag={}, event={}", topic, tag, event, e);
        }
    }

    @Override
    public String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (mqType != MQTypeEnum.EMQX) {
            return null;
        }
        // MQTT协议本身不支持延迟消息，通过DelayMessageSender实现
        String bodyStr = body instanceof String ? (String) body : JSON.toJSONString(body);
        String broadcastTopic = topic + ".broadcast";
        return delayMessageSender.sendDelayMessage(broadcastTopic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.EMQX;
    }
    
    @Override
    public boolean send(String topic, String tag, String body, Map<String, String> properties) {
        try {
            log.info("EMQX发送消息：topic={}, tag={}, body={}", topic, tag, body);
            
            // MQTT的topic格式：topic/tag
            String mqttTopic = topic + (tag != null && !tag.isEmpty() ? "/" + tag : "");
            
            MqttMessage mqttMessage = new MqttMessage(body.getBytes());
            mqttMessage.setQos(1); // 设置QoS级别为1，确保消息至少被传递一次
            
            // MQTT协议不支持在消息中添加自定义属性，如果需要可以将属性序列化到消息体中
            
            mqttClient.publish(mqttTopic, mqttMessage);
            return true;
        } catch (Exception e) {
            log.error("EMQX发送消息失败：topic={}, tag={}, body={}", topic, tag, body, e);
            return false;
        }
    }
}