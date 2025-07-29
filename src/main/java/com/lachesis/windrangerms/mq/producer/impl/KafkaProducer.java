package com.lachesis.windrangerms.mq.producer.impl;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.delay.DelayMessageSender;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * Kafka生产者实现
 */
@Slf4j
@Component
@ConditionalOnBean(org.apache.kafka.clients.producer.KafkaProducer.class)
public class KafkaProducer implements MQProducer {

    private final org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer;
    
    @Autowired(required = false)
    private DelayMessageSender delayMessageSender;

    public KafkaProducer(org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public String syncSend(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        try {
            String messageStr = convertToString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, tag, messageStr);
            
            Future<RecordMetadata> future = kafkaProducer.send(record);
            RecordMetadata metadata = future.get();
            
            log.info("Kafka消息发送成功: topic={}, tag={}, partition={}, offset={}", 
                    topic, tag, metadata.partition(), metadata.offset());
            return topic + "-" + metadata.partition() + "-" + metadata.offset();
        } catch (Exception e) {
            log.error("Kafka消息发送失败: topic={}, tag={}, message={}", topic, tag, event, e);
            throw new RuntimeException("Kafka消息发送失败", e);
        }
    }

    @Override
    public void asyncSend(MQTypeEnum mqType, String topic, String tag, Object event) {
        try {
            String messageStr = convertToString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, tag, messageStr);
            
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Kafka异步消息发送失败: topic={}, tag={}, message={}", topic, tag, event, exception);
                } else {
                    log.info("Kafka异步消息发送成功: topic={}, tag={}, partition={}, offset={}", 
                            topic, tag, metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            log.error("Kafka异步消息发送异常: topic={}, tag={}, message={}", topic, tag, event, e);
            throw new RuntimeException("Kafka异步消息发送异常", e);
        }
    }

    @Override
    public String syncSendBroadcast(MQTypeEnum mqType, String topic, String tag, MQEvent event) {
        // Kafka本身就是发布订阅模式，所以广播和普通发送一样
        return syncSend(mqType, topic, tag, event);
    }

    @Override
    public void asyncSendBroadcast(MQTypeEnum mqType, String topic, String tag, Object event) {
        // Kafka本身就是发布订阅模式，所以广播和普通发送一样
        asyncSend(mqType, topic, tag, event);
    }

    @Override
    public String asyncSendDelay(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        if (delayMessageSender != null) {
            String bodyStr = convertToString(body);
            String messageId = delayMessageSender.sendDelayMessage(topic, tag, bodyStr, getMQType().name(), delaySecond * 1000);
            log.info("Kafka延迟消息已提交: topic={}, tag={}, delayTime={}s, messageId={}", topic, tag, delaySecond, messageId);
            return messageId;
        } else {
            log.warn("DelayMessageSender未配置，无法发送延迟消息");
            throw new RuntimeException("DelayMessageSender未配置");
        }
    }

    @Override
    public String asyncSendDelayBroadcast(MQTypeEnum mqType, String topic, String tag, Object body, long delaySecond) {
        // Kafka本身就是发布订阅模式，所以广播和普通发送一样
        return asyncSendDelay(mqType, topic, tag, body, delaySecond);
    }

    @Override
    public boolean send(String topic, String tag, String body, Map<String, String> properties) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, tag, body);
            
            // 添加属性到headers
            if (properties != null) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    record.headers().add(entry.getKey(), entry.getValue().getBytes());
                }
            }
            
            Future<RecordMetadata> future = kafkaProducer.send(record);
            RecordMetadata metadata = future.get();
            
            log.info("Kafka消息发送成功: topic={}, tag={}, partition={}, offset={}", 
                    topic, tag, metadata.partition(), metadata.offset());
            return true;
        } catch (Exception e) {
            log.error("Kafka消息发送失败: topic={}, tag={}, body={}", topic, tag, body, e);
            return false;
        }
    }

    @Override
    public MQTypeEnum getMQType() {
        return MQTypeEnum.KAFKA;
    }

    /**
     * 将消息转换为字符串
     */
    private String convertToString(Object message) {
        if (message == null) {
            return null;
        }
        if (message instanceof String) {
            return (String) message;
        }
        return JSON.toJSONString(message);
    }

    /**
     * 关闭生产者
     */
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
            log.info("Kafka生产者已关闭");
        }
    }
}