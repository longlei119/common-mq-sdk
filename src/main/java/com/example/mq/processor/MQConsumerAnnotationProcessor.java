package com.example.mq.processor;

import com.example.mq.annotation.MQConsumer;
import com.example.mq.consumer.MQConsumerManager;
import com.example.mq.enums.MessageMode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
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
            
        } catch (Exception e) {
            log.error("注册MQ消费者失败: bean={}, method={}, error={}", 
                bean.getClass().getSimpleName(), method.getName(), e.getMessage(), e);
            throw new RuntimeException("注册MQ消费者失败", e);
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
        return message -> {
            try {
                method.setAccessible(true);
                method.invoke(bean, message);
            } catch (Exception e) {
                log.error("执行MQ消费者方法失败: bean={}, method={}, message={}, error={}", 
                    bean.getClass().getSimpleName(), method.getName(), message, e.getMessage(), e);
                throw new RuntimeException("执行MQ消费者方法失败", e);
            }
        };
    }
}