package com.lachesis.windrangerms.mq.delay.adapter;

import com.lachesis.windrangerms.mq.delay.model.DelayMessage;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * RabbitMQ适配器实现
 */
@Slf4j
public class RabbitMQAdapter implements MQAdapter {

    private final RabbitTemplate rabbitTemplate;

    public RabbitMQAdapter(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        log.info("RabbitMQ适配器初始化成功");
    }

    @Override
    public boolean send(DelayMessage message) {
        try {
            // 创建消息属性
            MessageProperties properties = new MessageProperties();
            properties.setMessageId(message.getId());
            
            // 设置消息属性
            if (message.getProperties() != null) {
                for (String key : message.getProperties().keySet()) {
                    properties.setHeader(key, message.getProperties().get(key));
                }
            }
            
            // 如果有标签，将其设置为路由键
            String routingKey = message.getTag() != null ? message.getTag() : "";
            
            // 创建RabbitMQ消息
            Message rabbitMessage = new Message(message.getBody().getBytes("UTF-8"), properties);
            
            // 发送消息
            rabbitTemplate.send(message.getTopic(), routingKey, rabbitMessage);
            
            log.info("RabbitMQ发送消息成功: messageId={}, exchange={}, routingKey={}", 
                    message.getId(), message.getTopic(), routingKey);
            return true;
        } catch (Exception e) {
            log.error("RabbitMQ发送消息异常: messageId={}, topic={}, tag={}, error={}", 
                    message.getId(), message.getTopic(), message.getTag(), e.getMessage(), e);
            return false;
        }
    }

    @Override
    public String getMQType() {
        return MQTypeEnum.RABBIT_MQ.getType();
    }
}