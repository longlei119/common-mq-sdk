package com.lachesis.windrangerms.mq.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lachesis.windrangerms.mq.annotation.MQConsumer;
import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.consumer.MQConsumerManager;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterStatusEnum;
import com.lachesis.windrangerms.mq.deadletter.model.RetryHistory;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.enums.MessageMode;
import com.lachesis.windrangerms.mq.enums.MessageAckResult;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * MQ消费者注解处理器
 * 自动扫描和注册带@MQConsumer注解的方法
 */
@Slf4j
//@Component
public class MQConsumerAnnotationProcessor implements BeanPostProcessor {
    
    @Autowired
    private MQConsumerManager mqConsumerManager;
    
    @Autowired
    private MQConfig mqConfig;
    
    @Autowired(required = false)
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
        Class<?> returnType = method.getReturnType();
        
        // 方法必须有且仅有一个String类型的参数
        if (parameterTypes.length != 1 || !String.class.equals(parameterTypes[0])) {
            throw new IllegalArgumentException(
                String.format("MQ消费者方法必须有且仅有一个String类型的参数: %s.%s", 
                    method.getDeclaringClass().getSimpleName(), method.getName()));
        }
        
        // 返回类型必须是void或MessageAckResult
        if (!returnType.equals(void.class) && !returnType.equals(MessageAckResult.class)) {
            throw new IllegalArgumentException(
                String.format("MQ消费者方法返回类型必须是void或MessageAckResult: %s.%s", 
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
        boolean hasReturnValue = method.getReturnType().equals(MessageAckResult.class);
        
        return message -> {
            // 生成或提取消息ID
            String messageId = extractMessageId(message);
            
            try {
                method.setAccessible(true);
                Object result = method.invoke(bean, message);
                
                // 处理返回值
                if (hasReturnValue) {
                    MessageAckResult ackResult = (MessageAckResult) result;
                    handleAckResult(messageId, message, annotation, ackResult, null);
                } else {
                    // void方法，消费成功，清除重试计数
                    messageRetryCountMap.remove(messageId);
                    // 删除死信队列中对应的消息（如果存在）
                    deleteFromDeadLetterQueue(messageId);
                }
                
            } catch (Exception e) {
                log.error("执行MQ消费者方法失败: bean={}, method={}, message={}, error={}", 
                    bean.getClass().getSimpleName(), method.getName(), message, e.getMessage(), e);
                
                if (hasReturnValue) {
                    // 有返回值的方法，异常时按RETRY处理
                    handleAckResult(messageId, message, annotation, MessageAckResult.RETRY, e.getMessage());
                } else {
                    // void方法，保持原有逻辑
                    handleVoidMethodException(messageId, message, annotation, e);
                }
            }
        };
    }
    
    /**
     * 处理ACK结果
     */
    private void handleAckResult(String messageId, String message, MQConsumer annotation, MessageAckResult ackResult, String errorMessage) {
        switch (ackResult) {
            case SUCCESS:
                // 消费成功，清除重试计数
                messageRetryCountMap.remove(messageId);
                // 删除死信队列中对应的消息（如果存在）
                deleteFromDeadLetterQueue(messageId);
                log.debug("消息处理成功: messageId={}", messageId);
                break;
                
            case RETRY:
                // 需要重试
                int retryCount = messageRetryCountMap.getOrDefault(messageId, 0) + 1;
                messageRetryCountMap.put(messageId, retryCount);
                
                boolean deadLetterEnabled = mqConfig.getDeadLetter() != null && mqConfig.getDeadLetter().isEnabled();
                int maxRetries = deadLetterEnabled ? mqConfig.getDeadLetter().getRetry().getMaxRetries() : 3;
                
                if (deadLetterEnabled && retryCount >= maxRetries) {
                    sendToDeadLetterQueue(messageId, message, annotation.mqType().getType(), annotation.topic(), annotation.tag(), errorMessage != null ? errorMessage : "重试次数超限");
                    messageRetryCountMap.remove(messageId);
                } else {
                    throw new RuntimeException("消息需要重试: " + (errorMessage != null ? errorMessage : "业务处理失败"));
                }
                break;
                
            case REJECT:
                // 拒绝消息，直接发送到死信队列
                boolean dlqEnabled = mqConfig.getDeadLetter() != null && mqConfig.getDeadLetter().isEnabled();
                if (dlqEnabled) {
                    sendToDeadLetterQueue(messageId, message, annotation.mqType().getType(), annotation.topic(), annotation.tag(), errorMessage != null ? errorMessage : "消息被拒绝");
                }
                messageRetryCountMap.remove(messageId);
                log.info("消息被拒绝: messageId={}", messageId);
                break;
                
            case IGNORE:
                // 忽略消息，清除重试计数但不做其他处理
                messageRetryCountMap.remove(messageId);
                // 删除死信队列中对应的消息（如果存在）
                deleteFromDeadLetterQueue(messageId);
                log.info("消息被忽略: messageId={}", messageId);
                break;
                
            default:
                log.warn("未知的ACK结果: {}, messageId={}", ackResult, messageId);
                messageRetryCountMap.remove(messageId);
                break;
        }
    }
    
    /**
     * 处理void方法的异常（保持向后兼容）
     */
    private void handleVoidMethodException(String messageId, String message, MQConsumer annotation, Exception e) {
        // 检查是否启用了死信队列
        boolean deadLetterEnabled = mqConfig.getDeadLetter() != null && mqConfig.getDeadLetter().isEnabled();
        int maxRetries = deadLetterEnabled ? mqConfig.getDeadLetter().getRetry().getMaxRetries() : 3;
        
        log.info("死信队列配置: enabled={}, maxRetries={}", deadLetterEnabled, maxRetries);
        
        // 对于Redis MQ，由于pub/sub不支持消息重试，直接发送到死信队列
        if (annotation.mqType() == MQTypeEnum.REDIS) {
            log.info("Redis消息处理失败，由于Redis pub/sub不支持重试，直接发送到死信队列: messageId={}", messageId);
            if (deadLetterEnabled) {
                sendToDeadLetterQueue(messageId, message, annotation.mqType().getType(), annotation.topic(), annotation.tag(), e.getMessage());
                messageRetryCountMap.remove(messageId);
                return; // Redis消息不抛出异常，避免影响其他消息处理
            }
        } else {
            // 对于其他MQ类型，使用正常的重试机制
            int retryCount = messageRetryCountMap.getOrDefault(messageId, 0) + 1;
            messageRetryCountMap.put(messageId, retryCount);
            
            log.info("消息处理失败: messageId={}, retryCount={}, message={}", messageId, retryCount, message);
            
            // 如果重试次数超过最大重试次数，将消息放入死信队列
            if (deadLetterEnabled && retryCount >= maxRetries) {
                log.info("消息重试次数超限，发送到死信队列: messageId={}, retryCount={}, maxRetries={}", messageId, retryCount, maxRetries);
                sendToDeadLetterQueue(messageId, message, annotation.mqType().getType(), annotation.topic(), annotation.tag(), e.getMessage());
                // 清除重试计数
                messageRetryCountMap.remove(messageId);
            } else {
                log.info("消息将继续重试: messageId={}, retryCount={}, maxRetries={}", messageId, retryCount, maxRetries);
            }
        }
        
        throw new RuntimeException("执行MQ消费者方法失败", e);
    }
    
    /**
     * 从消息中提取消息ID，如果没有则使用消息内容的哈希值作为稳定的ID
     */
    private String extractMessageId(String message) {
        log.debug("提取消息ID，原始消息: {}", message);
        
        try {
            // 首先尝试解析整个消息为JSON
            JSONObject jsonObject = JSON.parseObject(message);
            String messageId = jsonObject.getString("id");
            if (StringUtils.hasText(messageId)) {
                log.debug("从JSON消息中提取到ID: {}", messageId);
                return messageId;
            }
        } catch (Exception ignored) {
            // 解析失败，可能是Redis消息格式，尝试解析properties部分
        }
        
        // 处理Redis消息格式：properties JSON + "\n" + body
        try {
            if (message.contains("\n")) {
                String[] parts = message.split("\n", 2);
                if (parts.length >= 1) {
                    // 尝试解析第一部分为properties JSON
                    JSONObject properties = JSON.parseObject(parts[0]);
                    String messageId = properties.getString("messageId");
                    if (StringUtils.hasText(messageId)) {
                        log.debug("从Redis消息properties中提取到messageId: {}", messageId);
                        return messageId;
                    }
                }
            }
        } catch (Exception ignored) {
            // 解析失败，忽略
        }
        
        // 使用消息内容的哈希值作为稳定的ID，确保相同消息总是得到相同的ID
        String stableId = String.valueOf(message.hashCode());
        if (stableId.startsWith("-")) {
            stableId = "0" + stableId.substring(1); // 移除负号
        }
        log.debug("无法从消息中提取ID，使用消息哈希值作为稳定ID: {}", stableId);
        return stableId;
    }
    
    /**
     * 从死信队列中删除消息
     */
    private void deleteFromDeadLetterQueue(String messageId) {
        try {
            if (deadLetterServiceFactory == null) {
                log.debug("死信队列服务工厂不可用，跳过删除操作: messageId={}", messageId);
                return;
            }
            DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
            if (deadLetterService == null) {
                return;
            }
            
            // 查找死信队列中的消息
            List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 1000);
            for (DeadLetterMessage message : messages) {
                // 根据原始消息ID匹配
                if (messageId.equals(message.getOriginalMessageId()) || 
                    (message.getProperties() != null && messageId.equals(message.getProperties().get("messageId")))) {
                    boolean deleted = deadLetterService.deleteDeadLetterMessage(message.getId());
                    if (deleted) {
                        log.debug("已从死信队列删除消息: messageId={}, deadLetterMessageId={}", messageId, message.getId());
                    }
                    break;
                }
            }
        } catch (Exception e) {
            log.warn("从死信队列删除消息失败: messageId={}, error={}", messageId, e.getMessage());
        }
    }
    
    /**
     * 将消息发送到死信队列
     */
    private void sendToDeadLetterQueue(String messageId, String messageBody, String mqType, String topic, String tag, String failureReason) {
        try {
            if (deadLetterServiceFactory == null) {
                log.warn("死信队列服务工厂不可用，无法发送消息到死信队列: messageId={}", messageId);
                return;
            }
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
            // 设置原始字段
            deadLetterMessage.setOriginalTopic(topic);
            deadLetterMessage.setOriginalTag(tag);
            deadLetterMessage.setOriginalBody(messageBody);
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
            deadLetterMessage.setDeadLetterTime(System.currentTimeMillis());
            deadLetterMessage.setStatusEnum(DeadLetterStatusEnum.PENDING);
            
            // 创建重试历史
            List<RetryHistory> retryHistories = new ArrayList<>();
            RetryHistory retryHistory = new RetryHistory();
            retryHistory.setId(UUID.randomUUID().toString().replace("-", ""));
            retryHistory.setMessageId(deadLetterMessage.getId());
            retryHistory.setRetryTimestamp(System.currentTimeMillis());
            retryHistory.setSuccess(false);
            retryHistory.setFailureReason(failureReason);
            retryHistory.setRetryCount(messageRetryCountMap.getOrDefault(messageId, 0));
            retryHistories.add(retryHistory);
            deadLetterMessage.setRetryHistory(retryHistories);
            
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