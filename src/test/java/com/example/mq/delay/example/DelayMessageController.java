package com.example.mq.delay.example;

import com.example.mq.delay.DelayMessageSender;
import com.example.mq.delay.model.DelayMessage;
import com.example.mq.enums.MQTypeEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 延迟消息控制器示例
 * 展示如何在Web应用中使用延迟消息功能
 */
@RestController
@RequestMapping("/api/delay-message")
public class DelayMessageController {

    @Autowired
    private DelayMessageSender delayMessageSender;

    /**
     * 发送延迟消息
     * @param request 消息请求
     * @return 消息ID
     */
    @PostMapping("/send")
    public Map<String, Object> sendDelayMessage(@RequestBody DelayMessageRequest request) {
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId(UUID.randomUUID().toString());
        message.setTopic(request.getTopic());
        message.setTag(request.getTag());
        message.setBody(request.getBody());
        message.setMqTypeEnum(MQTypeEnum.fromString(request.getMqType()));
        
        // 计算投递时间戳
        long deliverTimestamp = System.currentTimeMillis() + request.getDelayMillis();
        message.setDeliverTimestamp(deliverTimestamp);
        
        // 设置消息属性
        if (request.getProperties() != null) {
            message.setProperties(request.getProperties());
        }
        
        // 发送延迟消息
        delayMessageSender.sendDelayMessage(message);
        
        // 返回结果
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("messageId", message.getId());
        result.put("deliverTime", deliverTimestamp);
        return result;
    }
    
    /**
     * 快速发送延迟消息
     * @param topic 主题
     * @param tag 标签
     * @param body 消息内容
     * @param mqType 消息队列类型
     * @param delayMillis 延迟毫秒数
     * @return 消息ID
     */
    @PostMapping("/send-simple")
    public Map<String, Object> sendSimpleDelayMessage(
            @RequestParam String topic,
            @RequestParam String tag,
            @RequestParam String body,
            @RequestParam String mqType,
            @RequestParam long delayMillis) {
        
        // 发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(topic, tag, body, mqType, delayMillis);
        
        // 返回结果
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("messageId", messageId);
        result.put("deliverTime", System.currentTimeMillis() + delayMillis);
        return result;
    }
}