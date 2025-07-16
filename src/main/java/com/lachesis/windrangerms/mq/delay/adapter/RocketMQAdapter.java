package com.lachesis.windrangerms.mq.delay.adapter;

import com.lachesis.windrangerms.mq.delay.model.DelayMessage;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

/**
 * RocketMQ适配器实现
 */
@Slf4j
public class RocketMQAdapter implements MQAdapter {

    private final DefaultMQProducer producer;
    private final String producerGroup;
    private final String nameServerAddr;

    public RocketMQAdapter(String producerGroup, String nameServerAddr) throws Exception {
        this.producerGroup = producerGroup;
        this.nameServerAddr = nameServerAddr;
        
        // 初始化RocketMQ生产者
        this.producer = new DefaultMQProducer(producerGroup);
        this.producer.setNamesrvAddr(nameServerAddr);
        this.producer.start();
        log.info("RocketMQ适配器初始化成功，producerGroup={}, nameServerAddr={}", producerGroup, nameServerAddr);
    }

    @Override
    public boolean send(DelayMessage message) {
        try {
            // 构建RocketMQ消息
            Message rocketMessage = new Message(
                    message.getTopic(),
                    message.getTag(),
                    message.getId(),
                    message.getBody().getBytes("UTF-8")
            );
            
            // 设置消息属性
            if (message.getProperties() != null) {
                for (String key : message.getProperties().keySet()) {
                    rocketMessage.putUserProperty(key, message.getProperties().get(key));
                }
            }
            
            // 发送消息
            SendResult sendResult = producer.send(rocketMessage);
            boolean success = sendResult.getSendStatus() == SendStatus.SEND_OK;
            
            if (success) {
                log.info("RocketMQ发送消息成功: messageId={}, topic={}, tag={}", 
                        message.getId(), message.getTopic(), message.getTag());
            } else {
                log.error("RocketMQ发送消息失败: messageId={}, topic={}, tag={}, status={}", 
                        message.getId(), message.getTopic(), message.getTag(), sendResult.getSendStatus());
            }
            
            return success;
        } catch (Exception e) {
            log.error("RocketMQ发送消息异常: messageId={}, topic={}, tag={}, error={}", 
                    message.getId(), message.getTopic(), message.getTag(), e.getMessage(), e);
            return false;
        }
    }

    @Override
    public String getMQType() {
        return MQTypeEnum.ROCKET_MQ.getType();
    }
}