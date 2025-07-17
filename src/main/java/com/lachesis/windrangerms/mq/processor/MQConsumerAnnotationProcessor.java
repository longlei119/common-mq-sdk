package com.lachesis.windrangerms.mq.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.consumer.MQConsumerManager;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.enums.MessageMode;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * MQ消费者注解处理器
 * 自动扫描和注册带@MQConsumer注解的方法
 */
@Slf4j
@Component
public class MQConsumerAnnotationProcessor implements BeanPostProcessor {
    
    @Autowired
    private MQConsumerManager mqConsumerManager;
    
    @Autowired
    private MQConfig mqConfig;
    
    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;
    
    // 消息重试计数器，记录每个消息的重试次数
    private final Map<String, Integer> messageRetryCountMap = new ConcurrentHashMap<>();
    
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> clazz = bean.getClass();
        
        // 扫描所有方法
        ReflectionUtils.doWithMethods(clazz, method -> {
            MQConsumer annotation = method.getAnnotation(MQConsumer.class);
            if (annotation != null) {
                registerMQConsumer(bean, method, annotation);
            }
        });
        
        return bean;
    }
    
    /**
     * 注册MQ消费者
     */
    private void registerMQConsumer(Object bean, Method method, MQConsumer annotation) {
        try {
            // 验证方法签名
            validateMethodSignature(method);

            // 创建消息处理器
            Consumer<String> messageHandler = createMessageHandler(bean, method);

            // 检查是否有对应 MQ 类型的实现
            if (mqConsumerManagerHasType(annotation.mqType())) {
                // 根据消息模式注册消费者
                if (annotation.mode() == MessageMode.UNICAST) {
                    mqConsumerManager.subscribeUnicast(
                        annotation.mqType(),
                        annotation.topic(),
                        annotation.tag(),
                        messageHandler,
                        annotation.consumerGroup()
                    );
                    log.info("注册单播消费者: bean={}, method={}, topic={}, tag={}, mqType={}", 
                        bean.getClass().getSimpleName(), method.getName(), 
                        annotation.topic(), annotation.tag(), annotation.mqType());
                } else {
                    mqConsumerManager.subscribeBroadcast(
                        annotation.mqType(),
                        annotation.topic(),
                        annotation.tag(),
                        messageHandler
                    );
                    log.info("注册广播消费者: bean={}, method={}, topic={}, tag={}, mqType={}", 
                        bean.getClass().getSimpleName(), method.getName(), 
                        annotation.topic(), annotation.tag(), annotation.mqType());
                }
            } else {
                log.warn("未检测到MQ类型{}的实现，跳过消费者注册: bean={}, method={}", annotation.mqType(), bean.getClass().getSimpleName(), method.getName());
            }
        } catch (Exception e) {
            log.error("注册MQ消费者失败: bean={}, method={}, error={}", 
                bean.getClass().getSimpleName(), method.getName(), e.getMessage(), e);
            throw new RuntimeException("注册MQ消费者失败", e);
        }
    }

    /**
     * 判断是否有对应 MQ 类型的实现
     */
    private boolean mqConsumerManagerHasType(com.lachesis.windrangerms.mq.enums.MQTypeEnum mqType) {
        try {
            java.lang.reflect.Field field = mqConsumerManager.getClass().getDeclaredField("consumerMap");
            field.setAccessible(true);
            java.util.Map<?,?> map = (java.util.Map<?,?>) field.get(mqConsumerManager);
            return map.containsKey(mqType);
        } catch (Exception e) {
            log.warn("检测MQ类型实现时异常: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 验证方法签名
     */
    private void validateMethodSignature(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        
        // 方法必须有且仅有一个String类型的参数
        if (parameterTypes.length != 1 || !String.class.equals(parameterTypes[0])) {
            throw new IllegalArgumentException(
                String.format("MQ消费者方法必须有且仅有一个String类型的参数: %s.%s", 
                    method.getDeclaringClass().getSimpleName(), method.getName()));
        }
        
        // 方法必须是public的
        if (!java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
            throw new IllegalArgumentException(
                String.format("MQ消费者方法必须是public的: %s.%s", 
                    method.getDeclaringClass().getSimpleName(), method.getName()));
        }
    }
    
    /**
     * 创建消息处理器
     */
    private Consumer<String> createMessageHandler(Object bean, Method method) {
        MQConsumer annotation = method.getAnnotation(MQConsumer.class);
        
        return message -> {
            // 生成或提取消息ID
            String messageId = extractMessageId(message);
            
            try {
                // 检查是否启用了死信队列
                boolean deadLetterEnabled = mqConfig.getDeadLetter() != null && mqConfig.getDeadLetter().isEnabled();
                
                // 获取当前消息的重试次数
                int retryCount = messageRetryCountMap.getOrDefault(messageId, 0);
                
                method.setAccessible(true);
                method.invoke(bean, message);
                
                // 消费成功，清除重试计数
                messageRetryCountMap.remove(messageId);
                
            } catch (Exception e) {
                log.error("执行MQ消费者方法失败: bean={}, method={}, message={}, error={}", 
                    bean.getClass().getSimpleName(), method.getName(), message, e.getMessage(), e);
                
                // 处理消息失败，增加重试计数
                int retryCount = messageRetryCountMap.getOrDefault(messageId, 0) + 1;
                messageRetryCountMap.put(messageId, retryCount);
                
                // 检查是否启用了死信队列
                boolean deadLetterEnabled = mqConfig.getDeadLetter() != null && mqConfig.getDeadLetter().isEnabled();
                int maxRetries = deadLetterEnabled ? mqConfig.getDeadLetter().getRetry().getMaxRetries() : 3;
                
                // 如果重试次数超过最大重试次数，将消息放入死信队列
                if (deadLetterEnabled && retryCount >= maxRetries) {
                    sendToDeadLetterQueue(messageId, message, annotation.mqType().name(), annotation.topic(), annotation.tag(), e.getMessage());
                    // 清除重试计数
                    messageRetryCountMap.remove(messageId);
                }
                
                throw new RuntimeException("执行MQ消费者方法失败", e);
            }
        };
    }
    
    /**
     * 从消息中提取消息ID，如果没有则生成一个
     */
    private String extractMessageId(String message) {
        try {
            JSONObject jsonObject = JSON.parseObject(message);
            String messageId = jsonObject.getString("id");
            if (StringUtils.hasText(messageId)) {
                return messageId;
            }
        } catch (Exception ignored) {
            // 解析失败，忽略
        }
        
        // 生成一个唯一ID
        return UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 将消息发送到死信队列
     */
    private void sendToDeadLetterQueue(String messageId, String messageBody, String mqType, String topic, String tag, String failureReason) {
        try {
            DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
            if (deadLetterService == null) {
                log.warn("死信队列服务不可用，无法发送消息到死信队列: messageId={}", messageId);
                return;
            }
            
            // 解析消息属性
            Map<String, Object> properties = new HashMap<>();
            try {
                JSONObject jsonObject = JSON.parseObject(messageBody);
                if (jsonObject.containsKey("properties")) {
                    Object propertiesObj = jsonObject.get("properties");
                    if (propertiesObj instanceof JSONObject) {
                        properties = ((JSONObject) propertiesObj).getInnerMap();
                    }
                }
            } catch (Exception ignored) {
                // 解析失败，使用空属性
            }
            
            // 创建死信消息
            DeadLetterMessage deadLetterMessage = new DeadLetterMessage();
            deadLetterMessage.setId(UUID.randomUUID().toString().replace("-", ""));
            deadLetterMessage.setOriginalMessageId(messageId);
            deadLetterMessage.setTopic(topic);
            deadLetterMessage.setTag(tag);
            deadLetterMessage.setBody(messageBody);
            // 转换properties类型
            Map<String, String> stringProperties = new HashMap<>();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                stringProperties.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
            }
            deadLetterMessage.setProperties(stringProperties);
            deadLetterMessage.setMqType(mqType);
            deadLetterMessage.setFailureReason(failureReason);
            deadLetterMessage.setRetryCount(0);
            deadLetterMessage.setCreateTimestamp(System.currentTimeMillis());
            
            // 保存到死信队列
            boolean result = deadLetterService.saveDeadLetterMessage(deadLetterMessage);
            if (result) {
                log.info("消息已发送到死信队列: messageId={}, topic={}, tag={}", messageId, topic, tag);
            } else {
                log.error("发送消息到死信队列失败: messageId={}, topic={}, tag={}", messageId, topic, tag);
            }
        } catch (Exception e) {
            log.error("发送消息到死信队列异常: messageId={}, error={}", messageId, e.getMessage(), e);
        }
    }
}