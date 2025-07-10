package com.example.mq.delay.example;

import com.example.mq.delay.model.DelayMessage;
import com.example.mq.delay.DelayMessageSender;
import com.example.mq.enums.MQTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 定时任务示例
 * 展示如何使用延迟消息实现定时任务
 */
@Component
public class ScheduledTaskExample {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledTaskExample.class);

    @Autowired
    private DelayMessageSender delayMessageSender;

    /**
     * 每天凌晨1点执行的定时任务
     * 发送延迟消息，用于处理当天的数据统计
     */
    @Scheduled(cron = "0 0 1 * * ?")
    public void dailyStatisticsTask() {
        logger.info("开始执行每日数据统计定时任务");
        
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId(UUID.randomUUID().toString());
        message.setTopic("statistics-topic");
        message.setTag("daily-statistics");
        message.setBody("执行每日数据统计任务");
        message.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        
        // 设置投递时间为5分钟后
        message.setDeliverTimestamp(System.currentTimeMillis() + 5 * 60 * 1000);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("taskType", "dailyStatistics");
        properties.put("executeDate", String.valueOf(System.currentTimeMillis()));
        message.setProperties(properties);
        
        // 发送延迟消息
        delayMessageSender.sendDelayMessage(message);
        
        logger.info("每日数据统计定时任务已调度，消息ID: {}", message.getId());
    }

    /**
     * 每周日晚上11点执行的定时任务
     * 发送延迟消息，用于处理每周的数据汇总
     */
    @Scheduled(cron = "0 0 23 ? * SUN")
    public void weeklyReportTask() {
        logger.info("开始执行每周报表生成定时任务");
        
        // 使用简化方法发送延迟消息
        String messageId = delayMessageSender.sendDelayMessage(
                "report-topic",
                "weekly-report",
                "执行每周报表生成任务",
                "ROCKET_MQ",
                10 * 60 * 1000  // 10分钟后执行
        );
        
        logger.info("每周报表生成定时任务已调度，消息ID: {}", messageId);
    }

    /**
     * 每小时执行一次的定时任务
     * 发送延迟消息，用于检查系统状态
     */
    @Scheduled(cron = "0 0 * * * ?")
    public void hourlySystemCheckTask() {
        logger.info("开始执行每小时系统检查定时任务");
        
        // 创建延迟消息
        DelayMessage message = new DelayMessage();
        message.setId(UUID.randomUUID().toString());
        message.setTopic("system-topic");
        message.setTag("system-check");
        message.setBody("执行系统状态检查任务");
        message.setMqTypeEnum(MQTypeEnum.ROCKET_MQ);
        
        // 设置投递时间为2分钟后
        message.setDeliverTimestamp(System.currentTimeMillis() + 2 * 60 * 1000);
        
        // 设置消息属性
        Map<String, String> properties = new HashMap<>();
        properties.put("taskType", "systemCheck");
        properties.put("checkTime", String.valueOf(System.currentTimeMillis()));
        message.setProperties(properties);
        
        // 发送延迟消息
        delayMessageSender.sendDelayMessage(message);
        
        logger.info("每小时系统检查定时任务已调度，消息ID: {}", message.getId());
    }
}