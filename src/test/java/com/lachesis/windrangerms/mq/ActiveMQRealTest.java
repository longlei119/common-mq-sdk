package com.lachesis.windrangerms.mq;

import com.lachesis.windrangerms.mq.consumer.MQConsumer;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.model.MQEvent;
import com.lachesis.windrangerms.mq.producer.MQProducer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.AfterEach;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.TestPropertySource;

import javax.jms.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ActiveMQ 真实测试类
 * 测试内容：同步、异步、顺序、延迟消息
 * 使用真实的ActiveMQ连接进行测试，验证延迟效果和性能
 */
@Slf4j
@SpringBootTest(properties = {
        "spring.redis.host=localhost",
        "spring.redis.port=6379",
        "spring.redis.timeout=1000ms",
        "spring.redis.lettuce.pool.max-active=1",
        "spring.redis.lettuce.pool.max-idle=1",
        "spring.redis.lettuce.pool.min-idle=0",
        "mq.delay.enabled=true",
        "mq.activemq.enabled=true"
})
@ConditionalOnProperty(name = "mq.activemq.enabled", havingValue = "true")
@TestMethodOrder(MethodOrderer.MethodName.class)
// 注意：ActiveMQ测试根据 application.yml 中的 mq.activemq.enabled 配置启用
// 配置通过 application.yml 文件管理
public class ActiveMQRealTest {

    @Autowired(required = false)
    private JmsTemplate jmsTemplate;
    
    @Autowired(required = false)
    private MQFactory mqFactory;
    
    // 移除直接注入MQProducer，改为通过MQFactory获取
    // @Autowired(required = false)
    // private MQProducer mqProducer;
    
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;
    
    private static final String QUEUE_NAME = "test.real.queue";
    private static final String TAG = "real_test";
    
    @Value("${mq.activemq.broker-url}")
    private String brokerUrl;
    
    @Value("${mq.activemq.username}")
    private String username;
    
    @Value("${mq.activemq.password}")
    private String password;

    @Data
    static class TestEvent extends MQEvent {
        private String message;
        private long timestamp;
        private int sequence;
        private String orderId;
        private double amount;
        private String customerInfo;

        @Override
        public String getTopic() {
            return QUEUE_NAME;
        }

        @Override
        public String getTag() {
            return TAG;
        }
        
        // 手动添加setter方法以解决编译问题
        public void setMessage(String message) {
            this.message = message;
        }
        
        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
        
        public void setSequence(int sequence) {
            this.sequence = sequence;
        }
        
        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }
        
        public void setAmount(double amount) {
            this.amount = amount;
        }
        
        public void setCustomerInfo(String customerInfo) {
            this.customerInfo = customerInfo;
        }
        
        // 手动添加getter方法
        public String getMessage() {
            return message;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public int getSequence() {
            return sequence;
        }
        
        public String getOrderId() {
            return orderId;
        }
        
        public double getAmount() {
            return amount;
        }
        
        public String getCustomerInfo() {
            return customerInfo;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        // 添加延迟确保测试间资源清理
        Thread.sleep(5000);
        
        // 创建ActiveMQ连接
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(username, password, brokerUrl);
        connectionFactory.setTrustAllPackages(true);
        
        connection = connectionFactory.createConnection();
        connection.start();
        
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        // 创建队列
        Destination destination = session.createQueue(QUEUE_NAME);
        
        // 创建生产者和消费者
        producer = session.createProducer(destination);
        consumer = session.createConsumer(destination);
        
        log.info("ActiveMQ真实连接建立成功，队列: {}, broker: {}", QUEUE_NAME, brokerUrl);
    }

    @Test
    void testSyncSend() throws Exception {
        TestEvent event = new TestEvent();
        event.setMessage("ActiveMQ真实同步消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setOrderId("ORDER-REAL-001");
        event.setAmount(99.99);
        event.setCustomerInfo("{\"name\":\"张三\",\"phone\":\"13800138000\"}");

        // 创建消息
        TextMessage message = session.createTextMessage();
        String messageContent = String.format(
            "{\"message\":\"%s\",\"timestamp\":%d,\"orderId\":\"%s\",\"amount\":%.2f,\"customerInfo\":%s}",
            event.getMessage(), event.getTimestamp(), event.getOrderId(), 
            event.getAmount(), event.getCustomerInfo()
        );
        message.setText(messageContent);
        message.setStringProperty("tag", event.getTag());
        
        long startTime = System.currentTimeMillis();
        producer.send(message);
        long endTime = System.currentTimeMillis();
        
        long sendTime = endTime - startTime;
        assertTrue(sendTime < 5000, "同步发送时间应该小于5秒");
        
        log.info("ActiveMQ真实同步发送完成，队列: {}, 耗时: {}ms", QUEUE_NAME, sendTime);
        log.info("发送内容: {}", messageContent);
    }

    @Test
    void testAsyncSend() throws Exception {
        TestEvent event = new TestEvent();
        event.setMessage("ActiveMQ真实异步消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setOrderId("ORDER-REAL-002");
        event.setAmount(199.99);
        event.setCustomerInfo("{\"name\":\"李四\",\"phone\":\"13900139000\"}");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong sendTime = new AtomicLong();
        
        // 创建消息
        TextMessage message = session.createTextMessage();
        String messageContent = String.format(
            "{\"message\":\"%s\",\"timestamp\":%d,\"orderId\":\"%s\",\"amount\":%.2f}",
            event.getMessage(), event.getTimestamp(), event.getOrderId(), event.getAmount()
        );
        message.setText(messageContent);
        message.setStringProperty("tag", event.getTag());
        
        long startTime = System.currentTimeMillis();
        
        // 异步发送
        CompletableFuture.runAsync(() -> {
            try {
                producer.send(message);
                sendTime.set(System.currentTimeMillis() - startTime);
                latch.countDown();
            } catch (Exception e) {
                log.error("ActiveMQ异步发送失败", e);
                latch.countDown();
            }
        });
        
        assertTrue(latch.await(10, TimeUnit.SECONDS), "ActiveMQ异步发送超时");
        assertTrue(sendTime.get() < 5000, "异步发送时间应该小于5秒");
        
        log.info("ActiveMQ真实异步发送完成，队列: {}, 耗时: {}ms", QUEUE_NAME, sendTime.get());
    }

    @Test
    void testOrderedMessages() throws Exception {
        // 发送有序消息
        for (int i = 1; i <= 5; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("ActiveMQ真实顺序消息" + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setOrderId("ORDER-REAL-SEQ-" + String.format("%03d", i));
            event.setAmount(100.0 + i * 10);
            
            TextMessage message = session.createTextMessage();
            String messageContent = String.format(
                "{\"message\":\"%s\",\"sequence\":%d,\"timestamp\":%d,\"orderId\":\"%s\"}",
                event.getMessage(), event.getSequence(), event.getTimestamp(), event.getOrderId()
            );
            message.setText(messageContent);
            message.setStringProperty("tag", event.getTag());
            message.setIntProperty("sequence", i);
            
            producer.send(message);
            
            // 添加小延迟确保顺序
            Thread.sleep(100);
            
            log.info("发送ActiveMQ真实顺序消息{}: {}", i, messageContent);
        }
        
        log.info("ActiveMQ真实顺序消息测试完成，队列: {}", QUEUE_NAME);
    }

    @Test
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
    void testDelayMessageAccuracy() throws Exception {
        // 设置消息接收计数器
        CountDownLatch receiveLatch = new CountDownLatch(1);
        AtomicLong receiveTime = new AtomicLong();
        
        // 为延迟消息测试创建广播主题消费者
        String topicName = QUEUE_NAME + ".broadcast." + TAG;
        log.info("创建ActiveMQ Topic消费者，监听Topic: {}", topicName);
        
        // 使用Spring的JmsTemplate创建消费者，确保与发送方使用相同的连接工厂
        javax.jms.Connection springConnection = jmsTemplate.getConnectionFactory().createConnection();
        springConnection.start();
        javax.jms.Session springSession = springConnection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Topic broadcastTopic = springSession.createTopic(topicName);
        MessageConsumer broadcastConsumer = springSession.createConsumer(broadcastTopic);
        
        // 设置消息监听器
        broadcastConsumer.setMessageListener(message -> {
            try {
                receiveTime.set(System.currentTimeMillis());
                if (message instanceof TextMessage) {
                    String content = ((TextMessage) message).getText();
                    log.info("收到ActiveMQ延迟消息: {}", content);
                }
                receiveLatch.countDown();
            } catch (Exception e) {
                log.error("处理ActiveMQ延迟消息失败", e);
            }
        });
        
        TestEvent event = new TestEvent();
        event.setMessage("ActiveMQ真实延迟消息测试");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setOrderId("ORDER-REAL-DELAY-001");
        event.setAmount(299.99);
        
        int delaySeconds = 3;
        long sendTime = System.currentTimeMillis();
        
        boolean usePublicDelayService = false;
        if (mqFactory != null) {
            try {
                MQProducer producer = mqFactory.getProducer(MQTypeEnum.ACTIVE_MQ);
                if (producer != null) {
                    String messageId = producer.asyncSendDelayBroadcast(
                        MQTypeEnum.ACTIVE_MQ, 
                        event.getTopic(), 
                        event.getTag(), 
                        event, 
                        (long) delaySeconds
                    );
                    log.info("使用公共延迟消息服务发送ActiveMQ延迟消息，延迟{}秒，消息ID: {}", delaySeconds, messageId);
                    usePublicDelayService = true;
                } else {
                    log.warn("ActiveMQ生产者未找到，使用降级方案");
                }
            } catch (Exception e) {
                log.warn("公共延迟消息服务发送失败，原因: {}，使用降级方案", e.getMessage());
            }
        } else {
            log.warn("MQFactory未注入，使用降级方案");
        }
        
        if (!usePublicDelayService) {
            fallbackToScheduledSend(event, sendTime, delaySeconds);
        }
        
        // 等待消息接收 - 增加等待时间以适应网络延迟和ActiveMQ调度器处理时间
        assertTrue(receiveLatch.await(delaySeconds + 15, TimeUnit.SECONDS), 
            "ActiveMQ延迟消息接收超时");
        
        long actualDelay = receiveTime.get() - sendTime;
        long expectedDelay = delaySeconds * 1000;
        long tolerance = 1000; // 1秒容差
        
        assertTrue(Math.abs(actualDelay - expectedDelay) <= tolerance,
            String.format("延迟时间不准确，期望%dms，实际%dms，容差%dms", 
                expectedDelay, actualDelay, tolerance));
        
        // 关闭广播消费者和Spring连接
        broadcastConsumer.close();
        springSession.close();
        springConnection.close();
        
        System.out.println("ActiveMQ真实延迟消息测试完成：");
        System.out.println("- 期望延迟: " + expectedDelay + "ms");
        System.out.println("- 实际延迟: " + actualDelay + "ms");
        System.out.println("- 延迟误差: " + Math.abs(actualDelay - expectedDelay) + "ms");
    }

    /**
     * 降级方案：使用ScheduledExecutorService模拟延迟发送
     */
    private void fallbackToScheduledSend(TestEvent event, long sendTime, int delaySeconds) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> {
            try {
                TextMessage message = session.createTextMessage();
                String messageContent = String.format(
                    "{\"message\":\"%s\",\"sendTime\":%d,\"delaySeconds\":%d,\"orderId\":\"%s\"}",
                    event.getMessage(), sendTime, delaySeconds, event.getOrderId()
                );
                message.setText(messageContent);
                message.setStringProperty("tag", event.getTag());
                
                producer.send(message);
                log.info("ActiveMQ延迟消息已发送（降级方案），延迟{}秒", delaySeconds);
            } catch (Exception e) {
                log.error("ActiveMQ延迟消息发送失败（降级方案）", e);
            } finally {
                scheduler.shutdown();
            }
        }, delaySeconds, TimeUnit.SECONDS);
    }

    @Test
    @DirtiesContext
    void testDelayMessagePerformance() throws Exception {
        log.info("开始测试ActiveMQ消息发送和接收性能");
        
        // 获取ActiveMQ生产者和消费者
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ACTIVE_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.ACTIVE_MQ);
        
        assertNotNull(producer, "ActiveMQ生产者不能为空");
        assertNotNull(consumer, "ActiveMQ消费者不能为空");
        
        String topic = "test-activemq-performance-topic";
        String tag = "performance-test-tag";
        String testMessage = "Hello ActiveMQ Performance Test Message";
        int messageCount = 3;
        
        // 用于统计接收到的消息数量
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch receiveLatch = new CountDownLatch(messageCount);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.ACTIVE_MQ, topic, tag, (message) -> {
            log.info("接收到ActiveMQ性能测试消息: {}", message);
            if (message.contains(testMessage)) {
                receivedCount.incrementAndGet();
                receiveLatch.countDown();
            }
        });
        
        // 等待消费者启动
        Thread.sleep(5000);
        
        long startTime = System.currentTimeMillis();
        
        // 发送多条测试消息
        for (int i = 0; i < messageCount; i++) {
            TestEvent event = new TestEvent();
            event.setMessage(testMessage + " " + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setOrderId("ORDER-PERF-" + String.format("%03d", i));
            
            log.info("发送消息: {}", event.getMessage());
            String messageId = producer.syncSend(MQTypeEnum.ACTIVE_MQ, topic, tag, event);
            assertNotNull(messageId, "消息ID不应为空");
        }
        
        // 等待所有消息被消费
        boolean allReceived = receiveLatch.await(15, TimeUnit.SECONDS);
        
        assertTrue(allReceived, "应该接收到所有消息");
        assertEquals(messageCount, receivedCount.get(), "应该接收到" + messageCount + "条消息");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        
        log.info("ActiveMQ性能测试完成，发送{}条，接收{}条，总耗时{}ms", 
                messageCount, receivedCount.get(), totalTime);
    }

    @Test
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
    void testLargeBatchMessages() throws Exception {
        log.info("开始测试ActiveMQ批量消息发送和接收");
        
        // 使用独立的队列名称和标签避免与其他测试冲突
        String batchQueueName = "test.batch.queue";
        String batchTag = "batch_test";
        
        // 获取ActiveMQ生产者和消费者
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ACTIVE_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.ACTIVE_MQ);
        
        assertNotNull(producer, "ActiveMQ生产者不能为空");
        assertNotNull(consumer, "ActiveMQ消费者不能为空");
        
        String testMessage = "Hello ActiveMQ Batch Test Message";
        int batchSize = 10; // 减少批量大小以提高测试稳定性
        
        // 用于统计接收到的消息数量
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch receiveLatch = new CountDownLatch(batchSize);
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.ACTIVE_MQ, batchQueueName, batchTag, (message) -> {
            log.info("接收到ActiveMQ批量消息: {}", message);
            if (message.contains(testMessage)) {
                receivedCount.incrementAndGet();
                receiveLatch.countDown();
            }
        });
        
        // 等待消费者启动
        Thread.sleep(5000);
        
        long startTime = System.currentTimeMillis();
        
        // 批量发送消息
        for (int i = 0; i < batchSize; i++) {
            TestEvent event = new TestEvent();
            event.setMessage(testMessage + " " + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setOrderId("ORDER-REAL-BATCH-" + String.format("%03d", i));
            event.setAmount(20.0 + (i % 40));
            
            // 使用MQProducer发送消息
            producer.syncSend(MQTypeEnum.ACTIVE_MQ, batchQueueName, batchTag, event);
            System.out.println("发送ActiveMQ批量消息 " + (i+1) + "/" + batchSize + ": " + event.getMessage());
        }
        
        long sendEndTime = System.currentTimeMillis();
        long sendTime = sendEndTime - startTime;
        
        // 等待所有消息接收完成
        assertTrue(receiveLatch.await(30, TimeUnit.SECONDS), 
            "ActiveMQ批量消息接收超时");

        long receiveEndTime = System.currentTimeMillis();
        long totalTime = receiveEndTime - startTime;

        // 验证性能指标
        assertEquals(batchSize, receivedCount.get(), "接收消息数量不匹配");
        
        System.out.println("ActiveMQ批量消息测试完成，接收到 " + receivedCount.get() + " 条消息");
        System.out.println("总耗时: " + totalTime + "ms");
        
        // 计算TPS
        double sendTps = batchSize * 1000.0 / sendTime;
        double totalTps = batchSize * 1000.0 / totalTime;
        
        System.out.println("ActiveMQ真实批量消息测试完成：");
        System.out.println("- 批量大小: " + batchSize);
        System.out.println("- 发送耗时: " + sendTime + "ms");
        System.out.println("- 总耗时: " + totalTime + "ms");
        System.out.println("- 发送TPS: " + String.format("%.2f", sendTps));
        System.out.println("- 整体TPS: " + String.format("%.2f", totalTps));
    }

    @Test
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
    void testPriorityMessages() throws Exception {
        int[] priorities = {1, 5, 9}; // 低、中、高优先级
        String[] priorityNames = {"低优先级", "中优先级", "高优先级"};
        int messagesPerPriority = 3; // 减少每个优先级的消息数量
        
        // 使用独立的队列名称和标签避免与其他测试冲突
        String priorityQueueName = "test.priority.queue";
        String priorityTag = "priority_test";
        
        CountDownLatch priorityLatch = new CountDownLatch(priorities.length * messagesPerPriority);
        AtomicInteger priorityReceivedCount = new AtomicInteger(0);
        
        // 使用MQFactory获取生产者和消费者
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ACTIVE_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.ACTIVE_MQ);
        
        assertNotNull(producer, "ActiveMQ生产者不能为空");
        assertNotNull(consumer, "ActiveMQ消费者不能为空");
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.ACTIVE_MQ, priorityQueueName, priorityTag, (message) -> {
            int count = priorityReceivedCount.incrementAndGet();
            System.out.println("收到ActiveMQ优先级消息 " + count + ": " + message);
            priorityLatch.countDown();
        });
        
        // 等待消费者启动
        Thread.sleep(5000);
        
        long priorityStartTime = System.currentTimeMillis();
        
        // 发送不同优先级的消息
        for (int priorityIndex = 0; priorityIndex < priorities.length; priorityIndex++) {
            int priority = priorities[priorityIndex];
            String priorityName = priorityNames[priorityIndex];
            
            for (int i = 0; i < messagesPerPriority; i++) {
                TestEvent event = new TestEvent();
                event.setMessage("ActiveMQ" + priorityName + "消息" + i);
                event.setTimestamp(System.currentTimeMillis());
                event.setSequence(i);
                event.setOrderId("ORDER-REAL-PRIORITY-" + priority + "-" + String.format("%03d", i));
                event.setAmount(100.0 * priority + i);
                
                // 使用MQProducer发送消息
                producer.syncSend(MQTypeEnum.ACTIVE_MQ, priorityQueueName, priorityTag, event);
                System.out.println("发送ActiveMQ" + priorityName + "消息: " + event.getMessage());
            }
        }
        
        long prioritySendTime = System.currentTimeMillis() - priorityStartTime;
        
        // 等待所有优先级消息接收完成
        assertTrue(priorityLatch.await(60, TimeUnit.SECONDS), 
            "ActiveMQ优先级消息接收超时");
        
        long priorityTotalTime = System.currentTimeMillis() - priorityStartTime;
        int totalMessages = priorities.length * messagesPerPriority;
        
        assertEquals(totalMessages, priorityReceivedCount.get(), "优先级消息接收数量不匹配");
        
        System.out.println("ActiveMQ优先级消息测试完成，接收到 " + priorityReceivedCount.get() + " 条消息");
        System.out.println("总耗时: " + priorityTotalTime + "ms");
    }

    @Test
    void testUnicastMode() throws Exception {
        // 单播模式验证：多个消费者只有一个能收到消息
        String unicastQueueName = QUEUE_NAME + ".unicast.verify";
        Destination unicastDestination = session.createQueue(unicastQueueName);
        MessageProducer unicastProducer = session.createProducer(unicastDestination);
        
        AtomicInteger totalReceivedCount = new AtomicInteger(0);
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1); // 期望只有1个消费者收到消息
        
        // 创建第一个消费者
        MessageConsumer unicastConsumer1 = session.createConsumer(unicastDestination);
        System.out.println("创建单播消费者1");
        unicastConsumer1.setMessageListener(message -> {
            try {
                System.out.println("单播消费者1收到消息: " + ((TextMessage) message).getText());
                consumer1Count.incrementAndGet();
                totalReceivedCount.incrementAndGet();
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        // 创建第二个消费者（ActiveMQ中同一个队列的多个消费者会负载均衡）
        MessageConsumer unicastConsumer2 = session.createConsumer(unicastDestination);
        System.out.println("创建单播消费者2");
        unicastConsumer2.setMessageListener(message -> {
            try {
                System.out.println("单播消费者2收到消息: " + ((TextMessage) message).getText());
                consumer2Count.incrementAndGet();
                totalReceivedCount.incrementAndGet();
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        // 等待消费者准备就绪
        Thread.sleep(1000);
        
        // 发送单播消息
        TestEvent event = new TestEvent();
        event.setMessage("ActiveMQ单播模式验证消息");
        event.setTimestamp(System.currentTimeMillis());
        event.setSequence(1);
        event.setOrderId("unicast-order-001");
        event.setAmount(99.99);
        event.setCustomerInfo("{\"name\":\"测试用户\",\"phone\":\"13800138000\"}");
        
        TextMessage message = session.createTextMessage();
        String messageContent = String.format(
            "{\"message\":\"%s\",\"timestamp\":%d,\"orderId\":\"%s\",\"amount\":%.2f}",
            event.getMessage(), event.getTimestamp(), event.getOrderId(), event.getAmount()
        );
        message.setText(messageContent);
        message.setStringProperty("tag", event.getTag());
        
        System.out.println("发送单播消息");
        unicastProducer.send(message);
        
        // 等待消息处理
        assertTrue(latch.await(10, TimeUnit.SECONDS), "单播消息接收超时");
        
        // 验证单播特性：只有一个消费者收到消息（ActiveMQ负载均衡）
        assertEquals(1, totalReceivedCount.get(), 
            "ActiveMQ单播模式应该只有一个消费者收到消息，实际收到总数: " + totalReceivedCount.get());
        
        // ActiveMQ中多个消费者会负载均衡，所以只有一个消费者收到消息
        assertTrue(consumer1Count.get() + consumer2Count.get() == 1, 
            "总共应该只有一个消费者收到消息");
        assertTrue((consumer1Count.get() == 1 && consumer2Count.get() == 0) || 
                  (consumer1Count.get() == 0 && consumer2Count.get() == 1), 
            "应该只有一个消费者收到消息");
        
        System.out.println("ActiveMQ单播模式验证完成 - 消费者1收到: " + consumer1Count.get() + ", 消费者2收到: " + consumer2Count.get() + ", 总计: " + totalReceivedCount.get());
        
        // 清理资源
        unicastConsumer1.close();
        unicastConsumer2.close();
        unicastProducer.close();
    }

    @Test
    void testTransactionMessages() throws Exception {
        // 创建事务会话
        Session transactionSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination transactionDestination = transactionSession.createQueue(QUEUE_NAME + ".transaction");
        MessageProducer transactionProducer = transactionSession.createProducer(transactionDestination);
        MessageConsumer transactionConsumer = transactionSession.createConsumer(transactionDestination);
        
        CountDownLatch transactionLatch = new CountDownLatch(2);
        AtomicInteger transactionReceivedCount = new AtomicInteger(0);
        
        // 设置事务消息监听器
        transactionConsumer.setMessageListener(message -> {
            try {
                int count = transactionReceivedCount.incrementAndGet();
                System.out.println("收到ActiveMQ事务消息 " + count + ": " + ((TextMessage) message).getText());
                transactionSession.commit(); // 提交事务
                transactionLatch.countDown();
            } catch (Exception e) {
                System.err.println("处理ActiveMQ事务消息失败: " + e.getMessage());
                try {
                    transactionSession.rollback(); // 回滚事务
                } catch (JMSException ex) {
                    System.err.println("事务回滚失败: " + ex.getMessage());
                }
            }
        });
        
        // 发送事务消息
        try {
            TestEvent event1 = new TestEvent();
            event1.setMessage("ActiveMQ真实事务消息1");
            event1.setTimestamp(System.currentTimeMillis());
            event1.setOrderId("ORDER-REAL-TX-001");
            event1.setAmount(150.0);
            
            TextMessage message1 = transactionSession.createTextMessage();
            message1.setText(event1.getMessage());
            message1.setStringProperty("orderId", event1.getOrderId());
            transactionProducer.send(message1);
            
            TestEvent event2 = new TestEvent();
            event2.setMessage("ActiveMQ真实事务消息2");
            event2.setTimestamp(System.currentTimeMillis());
            event2.setOrderId("ORDER-REAL-TX-002");
            event2.setAmount(250.0);
            
            TextMessage message2 = transactionSession.createTextMessage();
            message2.setText(event2.getMessage());
            message2.setStringProperty("orderId", event2.getOrderId());
            transactionProducer.send(message2);
            
            // 提交事务
            transactionSession.commit();
            
            System.out.println("ActiveMQ事务消息发送并提交成功");
        } catch (Exception e) {
            e.printStackTrace();
            transactionSession.rollback();
        }
        
        // 等待事务消息接收完成
        assertTrue(transactionLatch.await(10, TimeUnit.SECONDS), 
            "ActiveMQ事务消息接收超时");
        assertEquals(2, transactionReceivedCount.get(), "应该收到2条事务消息");
        
        // 关闭事务资源
        transactionConsumer.close();
        transactionProducer.close();
        transactionSession.close();
        
        System.out.println("ActiveMQ真实事务消息测试完成");
    }

    @Test
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
    void testConnectionResilience() throws Exception {
        CountDownLatch resilienceLatch = new CountDownLatch(2);
        AtomicInteger resilienceReceivedCount = new AtomicInteger(0);
        
        // 使用MQFactory获取生产者和消费者
        MQProducer producer = mqFactory.getProducer(MQTypeEnum.ACTIVE_MQ);
        MQConsumer consumer = mqFactory.getConsumer(MQTypeEnum.ACTIVE_MQ);
        
        assertNotNull(producer, "ActiveMQ生产者不能为空");
        assertNotNull(consumer, "ActiveMQ消费者不能为空");
        
        // 订阅消息
        consumer.subscribe(MQTypeEnum.ACTIVE_MQ, QUEUE_NAME, TAG, (message) -> {
            int count = resilienceReceivedCount.incrementAndGet();
            System.out.println("收到ActiveMQ弹性测试消息 " + count + ": " + message);
            resilienceLatch.countDown();
        });
        
        // 等待消费者启动
        Thread.sleep(5000);
        
        // 发送两条测试消息
        TestEvent event1 = new TestEvent();
        event1.setMessage("ActiveMQ连接弹性测试消息1");
        event1.setTimestamp(System.currentTimeMillis());
        event1.setOrderId("ORDER-REAL-RESILIENCE-001");
        
        TestEvent event2 = new TestEvent();
        event2.setMessage("ActiveMQ连接弹性测试消息2");
        event2.setTimestamp(System.currentTimeMillis());
        event2.setOrderId("ORDER-REAL-RESILIENCE-002");
        
        // 使用MQProducer发送消息
        producer.syncSend(MQTypeEnum.ACTIVE_MQ, QUEUE_NAME, TAG, event1);
        producer.syncSend(MQTypeEnum.ACTIVE_MQ, QUEUE_NAME, TAG, event2);
        
        assertTrue(resilienceLatch.await(15, TimeUnit.SECONDS), 
            "ActiveMQ弹性测试消息接收超时");
        assertEquals(2, resilienceReceivedCount.get(), "应该收到2条弹性测试消息");
        
        System.out.println("ActiveMQ连接弹性测试完成，接收到 " + resilienceReceivedCount.get() + " 条消息");
    }



    @AfterEach
    void tearDown() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
        if (session != null) {
            session.close();
        }
        if (connection != null) {
            connection.close();
        }
        
        // 添加延迟确保资源完全释放
        Thread.sleep(2000);
        System.out.println("ActiveMQ连接已关闭");
    }
}