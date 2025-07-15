package com.example.mq.delay.adapter;

import com.example.mq.delay.model.DelayMessage;
import com.example.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Map;

/**
 * ActiveMQ适配器实现
 */
@Slf4j
public class ActiveMQAdapter implements MQAdapter {

    private final JmsTemplate jmsTemplate;

    public ActiveMQAdapter(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
        log.info("ActiveMQ适配器初始化成功");
    }

    @Override
    public boolean send(DelayMessage message) {
        try {
            String destination = message.getTopic();
            if (message.getTag() != null && !message.getTag().isEmpty()) {
                destination += "." + message.getTag();
            }
            
            // 使用JmsTemplate发送消息
            jmsTemplate.send(destination, session -> {
                try {
                    BytesMessage bytesMessage = session.createBytesMessage();
                    bytesMessage.writeBytes(message.getBody().getBytes("UTF-8"));
                    
                    // 设置消息ID
                    bytesMessage.setStringProperty("messageId", message.getId());
                    
                    // 设置消息属性
                    if (message.getProperties() != null) {
                        for (Map.Entry<String, String> entry : message.getProperties().entrySet()) {
                            bytesMessage.setStringProperty(entry.getKey(), entry.getValue());
                        }
                    }
                    
                    return bytesMessage;
                } catch (Exception e) {
                    throw new RuntimeException("创建消息失败", e);
                }
            });
            
            log.info("ActiveMQ发送消息成功: messageId={}, destination={}", 
                    message.getId(), destination);
            return true;
        } catch (Exception e) {
            log.error("ActiveMQ发送消息异常: messageId={}, topic={}, tag={}, error={}", 
                    message.getId(), message.getTopic(), message.getTag(), e.getMessage(), e);
            return false;
        }
    }

    @Override
    public String getMQType() {
        return MQTypeEnum.ACTIVE_MQ.getType();
    }
}