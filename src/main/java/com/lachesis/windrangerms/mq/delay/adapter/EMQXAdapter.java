package com.lachesis.windrangerms.mq.delay.adapter;

import com.lachesis.windrangerms.mq.delay.model.DelayMessage;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * EMQX适配器实现
 * 用于延迟消息发送到EMQX
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
public class EMQXAdapter implements MQAdapter {

    private final MqttClient mqttClient;

    public EMQXAdapter(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
        log.info("EMQX适配器初始化成功");
    }

    @Override
    public boolean send(DelayMessage message) {
        try {
            // 构建MQTT消息
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setPayload(message.getBody().getBytes());
            mqttMessage.setQos(1); // 设置QoS为1，确保消息至少被传递一次
            mqttMessage.setRetained(false);
            
            // MQTT的topic格式：topic/tag
            String mqttTopic = message.getTag() != null ? 
                message.getTopic() + "/" + message.getTag() : message.getTopic();
            
            // 发送消息
            mqttClient.publish(mqttTopic, mqttMessage);
            
            log.info("EMQX延迟消息发送成功：topic={}, tag={}, messageId={}", 
                message.getTopic(), message.getTag(), message.getId());
            return true;
        } catch (Exception e) {
            log.error("EMQX延迟消息发送失败：topic={}, tag={}, messageId={}", 
                message.getTopic(), message.getTag(), message.getId(), e);
            return false;
        }
    }

    @Override
    public String getMQType() {
        return MQTypeEnum.EMQX.getType();
    }
}