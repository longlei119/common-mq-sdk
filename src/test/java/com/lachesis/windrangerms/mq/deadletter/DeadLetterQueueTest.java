package com.lachesis.windrangerms.mq.deadletter;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterService;
import com.lachesis.windrangerms.mq.deadletter.DeadLetterServiceFactory;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterStatusEnum;
import com.lachesis.windrangerms.mq.deadletter.model.RetryHistory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 死信队列功能测试类
 */
@SpringBootTest
public class DeadLetterQueueTest {

    @Autowired
    private DeadLetterServiceFactory deadLetterServiceFactory;

    /**
     * 测试保存和获取死信消息
     */
    @Test
    public void testSaveAndGetDeadLetterMessage() {
        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信队列服务不应为空");

        // 创建测试消息
        String messageId = UUID.randomUUID().toString().replace("-", "");
        DeadLetterMessage message = createTestMessage(messageId);

        // 保存消息
        boolean saveResult = deadLetterService.saveDeadLetterMessage(message);
        assertTrue(saveResult, "保存死信消息应成功");

        // 获取消息
        DeadLetterMessage retrievedMessage = deadLetterService.getDeadLetterMessage(messageId);
        assertNotNull(retrievedMessage, "获取的死信消息不应为空");
        assertEquals(messageId, retrievedMessage.getId(), "消息ID应匹配");
        assertEquals("test-topic", retrievedMessage.getTopic(), "主题应匹配");
        assertEquals("test-tag", retrievedMessage.getTag(), "标签应匹配");
        assertEquals("ROCKET_MQ", retrievedMessage.getMqType(), "MQ类型应匹配");
        assertEquals(DeadLetterStatusEnum.PENDING, retrievedMessage.getStatusEnum(), "状态应为PENDING");
    }

    /**
     * 测试重新投递死信消息
     */
    @Test
    public void testRedeliverMessage() {
        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信队列服务不应为空");

        // 创建测试消息
        String messageId = UUID.randomUUID().toString().replace("-", "");
        DeadLetterMessage message = createTestMessage(messageId);

        // 保存消息
        boolean saveResult = deadLetterService.saveDeadLetterMessage(message);
        assertTrue(saveResult, "保存死信消息应成功");

        // 重新投递消息
        boolean redeliverResult = deadLetterService.redeliverMessage(messageId);
        // 注意：由于测试环境可能没有实际的MQ生产者，重新投递可能会失败
        System.out.println("重新投递结果: " + redeliverResult);

        // 获取重试历史
        List<RetryHistory> retryHistories = deadLetterService.getRetryHistory(messageId);
        assertNotNull(retryHistories, "重试历史不应为空");
        assertFalse(retryHistories.isEmpty(), "重试历史不应为空");

        // 打印重试历史
        for (RetryHistory history : retryHistories) {
            System.out.println("重试历史: " + JSON.toJSONString(history));
        }
    }

    /**
     * 测试列出和计数死信消息
     */
    @Test
    public void testListAndCountDeadLetterMessages() {
        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信队列服务不应为空");

        // 创建多个测试消息
        for (int i = 0; i < 5; i++) {
            String messageId = UUID.randomUUID().toString().replace("-", "");
            DeadLetterMessage message = createTestMessage(messageId);
            deadLetterService.saveDeadLetterMessage(message);
        }

        // 获取消息列表
        List<DeadLetterMessage> messages = deadLetterService.listDeadLetterMessages(0, 10);
        assertNotNull(messages, "消息列表不应为空");
        System.out.println("获取到的消息数量: " + messages.size());

        // 获取消息总数
        long count = deadLetterService.countDeadLetterMessages();
        System.out.println("消息总数: " + count);
        assertTrue(count > 0, "消息总数应大于0");
    }

    /**
     * 测试删除死信消息
     */
    @Test
    public void testDeleteDeadLetterMessage() {
        DeadLetterService deadLetterService = deadLetterServiceFactory.getDeadLetterService();
        assertNotNull(deadLetterService, "死信队列服务不应为空");

        // 创建测试消息
        String messageId = UUID.randomUUID().toString().replace("-", "");
        DeadLetterMessage message = createTestMessage(messageId);

        // 保存消息
        boolean saveResult = deadLetterService.saveDeadLetterMessage(message);
        assertTrue(saveResult, "保存死信消息应成功");

        // 删除消息
        boolean deleteResult = deadLetterService.deleteDeadLetterMessage(messageId);
        assertTrue(deleteResult, "删除死信消息应成功");

        // 确认消息已删除
        DeadLetterMessage deletedMessage = deadLetterService.getDeadLetterMessage(messageId);
        assertNull(deletedMessage, "删除后的消息应为空");
    }

    /**
     * 创建测试消息
     */
    private DeadLetterMessage createTestMessage(String messageId) {
        DeadLetterMessage message = new DeadLetterMessage();
        message.setId(messageId);
        message.setOriginalMessageId(UUID.randomUUID().toString().replace("-", ""));
        message.setTopic("test-topic");
        message.setTag("test-tag");
        message.setBody("{\"content\":\"测试消息内容\",\"timestamp\":" + System.currentTimeMillis() + "}");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("testKey", "testValue");
        properties.put("timestamp", System.currentTimeMillis()+"");
        message.setProperties(properties);
        
        message.setMqType("ROCKET_MQ");
        message.setFailureReason("测试失败原因");
        message.setRetryCount(0);
        message.setStatusEnum(DeadLetterStatusEnum.PENDING);
        message.setCreateTimestamp(System.currentTimeMillis());
        
        return message;
    }
}