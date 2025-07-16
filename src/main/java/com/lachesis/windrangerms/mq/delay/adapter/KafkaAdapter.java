package com.lachesis.windrangerms.mq.delay.adapter;

import com.lachesis.windrangerms.mq.delay.model.DelayMessage;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Kafka适配器实现
 */
@Slf4j
public class KafkaAdapter implements MQAdapter {

    private final KafkaProducer<String, byte[]> producer;

    public KafkaAdapter(KafkaProducer<String, byte[]> producer) {
        this.producer = producer;
        log.info("Kafka适配器初始化成功");
    }

    @Override
    public boolean send(DelayMessage message) {
        try {
            // 构建Kafka消息头
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader("messageId", message.getId().getBytes(StandardCharsets.UTF_8)));
            
            // 添加消息属性到头部
            if (message.getProperties() != null) {
                for (Map.Entry<String, String> entry : message.getProperties().entrySet()) {
                    headers.add(new RecordHeader(entry.getKey(), 
                            entry.getValue().getBytes(StandardCharsets.UTF_8)));
                }
            }
            
            // 构建Kafka消息记录
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    message.getTopic(),
                    null,  // 分区，使用默认分区策略
                    message.getId(),  // 键
                    message.getBody().getBytes("UTF-8"),  // 值
                    headers
            );
            
            // 发送消息
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            
            log.info("Kafka发送消息成功: messageId={}, topic={}, partition={}, offset={}", 
                    message.getId(), metadata.topic(), metadata.partition(), metadata.offset());
            return true;
        } catch (Exception e) {
            log.error("Kafka发送消息异常: messageId={}, topic={}, tag={}, error={}", 
                    message.getId(), message.getTopic(), message.getTag(), e.getMessage(), e);
            return false;
        }
    }

    @Override
    public String getMQType() {
        return MQTypeEnum.KAFKA.getType();
    }
}