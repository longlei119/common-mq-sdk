package com.example.mq.delay.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 延迟消息示例应用
 * 展示如何集成和使用延迟消息功能
 */
@SpringBootApplication
@EnableScheduling
public class DelayMessageExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(DelayMessageExampleApplication.class, args);
    }
}