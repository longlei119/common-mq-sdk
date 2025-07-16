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
import org.springframework.beans.factory.annotation.Autowired;
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
@SpringBootTest
@ConditionalOnProperty(name = "mq.activemq.enabled", havingValue = "true")
// 注意：ActiveMQ测试根据 application.yml 中的 mq.activemq.enabled 配置启用
// 配置通过 application.yml 文件管理
public class ActiveMQRealTest {

    @Autowired(required = false)
    private JmsTemplate jmsTemplate;
    
    @Autowired(required = false)
    private MQFactory mqFactory;
    
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;
    
    private static final String QUEUE_NAME = "test.real.queue";
    private static final String TAG = "real_test";
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";

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
        // 创建ActiveMQ连接
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKER_URL);
        connectionFactory.setTrustAllPackages(true);
        
        connection = connectionFactory.createConnection();
        connection.start();
        
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        // 创建队列
        Destination destination = session.createQueue(QUEUE_NAME);
        
        // 创建生产者和消费者
        producer = session.createProducer(destination);
        consumer = session.createConsumer(destination);
        
        log.info("ActiveMQ真实连接建立成功，队列: {}", QUEUE_NAME);
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
    void testDelayMessageAccuracy() throws Exception {
        // 设置消息接收计数器
        CountDownLatch receiveLatch = new CountDownLatch(1);
        AtomicLong receiveTime = new AtomicLong();
        
        // 设置消息监听器
        consumer.setMessageListener(message -> {
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
        
        // 使用定时器模拟延迟发送（ActiveMQ本身支持延迟消息）
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
                log.info("ActiveMQ延迟消息已发送，延迟{}秒", delaySeconds);
            } catch (Exception e) {
                log.error("ActiveMQ延迟消息发送失败", e);
            }
        }, delaySeconds, TimeUnit.SECONDS);
        
        // 等待消息接收
        assertTrue(receiveLatch.await(delaySeconds + 5, TimeUnit.SECONDS), 
            "ActiveMQ延迟消息接收超时");
        
        long actualDelay = receiveTime.get() - sendTime;
        long expectedDelay = delaySeconds * 1000;
        long tolerance = 1000; // 1秒容差
        
        assertTrue(Math.abs(actualDelay - expectedDelay) <= tolerance,
            String.format("延迟时间不准确，期望%dms，实际%dms，容差%dms", 
                expectedDelay, actualDelay, tolerance));
        
        scheduler.shutdown();
        
        log.info("ActiveMQ真实延迟消息测试完成：");
        log.info("- 期望延迟: {}ms", expectedDelay);
        log.info("- 实际延迟: {}ms", actualDelay);
        log.info("- 延迟误差: {}ms", Math.abs(actualDelay - expectedDelay));
    }

    @Test
    void testDelayMessagePerformance() throws Exception {
        int messageCount = 50;
        int[] delayIntervals = {1, 2, 3, 5, 8}; // 不同延迟时间段
        
        CountDownLatch receiveLatch = new CountDownLatch(messageCount);
        AtomicInteger receivedCount = new AtomicInteger(0);
        ConcurrentHashMap<String, Long> messageReceiveTimes = new ConcurrentHashMap<>();
        
        // 设置消息监听器
        consumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    String content = ((TextMessage) message).getText();
                    messageReceiveTimes.put(content, System.currentTimeMillis());
                    int count = receivedCount.incrementAndGet();
                    log.debug("收到ActiveMQ性能测试消息 {}/{}: {}", count, messageCount, content);
                }
                receiveLatch.countDown();
            } catch (Exception e) {
                log.error("处理ActiveMQ性能测试消息失败", e);
            }
        });
        
        long startTime = System.currentTimeMillis();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        
        // 发送大量不同延迟时间的消息
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            int delaySeconds = delayIntervals[i % delayIntervals.length];
            
            TestEvent event = new TestEvent();
            event.setMessage("ActiveMQ性能测试消息" + index);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(index);
            event.setOrderId("ORDER-REAL-PERF-" + String.format("%03d", index));
            event.setAmount(20.0 + (index % 50));
            
            scheduler.schedule(() -> {
                try {
                    TextMessage message = session.createTextMessage();
                    String messageContent = String.format(
                        "{\"message\":\"%s\",\"index\":%d,\"delaySeconds\":%d,\"sendTime\":%d}",
                        event.getMessage(), index, delaySeconds, System.currentTimeMillis()
                    );
                    message.setText(messageContent);
                    message.setStringProperty("tag", event.getTag());
                    
                    producer.send(message);
                } catch (Exception e) {
                    log.error("ActiveMQ性能测试消息{}发送失败", index, e);
                }
            }, delaySeconds, TimeUnit.SECONDS);
        }
        
        // 等待所有消息接收完成（最大延迟时间 + 额外等待时间）
        int maxDelay = delayIntervals[delayIntervals.length - 1];
        assertTrue(receiveLatch.await(maxDelay + 10, TimeUnit.SECONDS), 
            "ActiveMQ性能测试消息接收超时");
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        
        // 计算性能指标
        assertEquals(messageCount, receivedCount.get(), "接收消息数量不匹配");
        
        scheduler.shutdown();
        
        log.info("ActiveMQ真实延迟消息性能测试完成：");
        log.info("- 发送消息数: {}", messageCount);
        log.info("- 接收消息数: {}", receivedCount.get());
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- 不同延迟时间段: {} 秒", java.util.Arrays.toString(delayIntervals));
    }

    @Test
    void testLargeBatchMessages() throws Exception {
        int batchSize = 100;
        CountDownLatch receiveLatch = new CountDownLatch(batchSize);
        AtomicInteger receivedCount = new AtomicInteger(0);
        
        // 设置消息监听器
        consumer.setMessageListener(message -> {
            int count = receivedCount.incrementAndGet();
            log.debug("收到ActiveMQ批量消息 {}/{}", count, batchSize);
            receiveLatch.countDown();
        });
        
        long startTime = System.currentTimeMillis();
        
        // 批量发送消息
        for (int i = 0; i < batchSize; i++) {
            TestEvent event = new TestEvent();
            event.setMessage("ActiveMQ批量消息" + i);
            event.setTimestamp(System.currentTimeMillis());
            event.setSequence(i);
            event.setOrderId("ORDER-REAL-BATCH-" + String.format("%03d", i));
            event.setAmount(20.0 + (i % 40));
            
            TextMessage message = session.createTextMessage();
            String messageContent = String.format(
                "{\"message\":\"%s\",\"index\":%d,\"timestamp\":%d,\"orderId\":\"%s\"}",
                event.getMessage(), i, event.getTimestamp(), event.getOrderId()
            );
            message.setText(messageContent);
            message.setStringProperty("tag", event.getTag());
            
            producer.send(message);
        }
        
        long sendEndTime = System.currentTimeMillis();
        long sendTime = sendEndTime - startTime;
        
        // 等待所有消息接收完成
        assertTrue(receiveLatch.await(30, TimeUnit.SECONDS), 
            "ActiveMQ批量消息接收超时");
        
        long receiveEndTime = System.currentTimeMillis();
        long totalTime = receiveEndTime - startTime;
        
        double sendTps = (double) batchSize / (sendTime / 1000.0);
        double totalTps = (double) batchSize / (totalTime / 1000.0);
        
        // 验证性能指标
        assertEquals(batchSize, receivedCount.get(), "接收消息数量不匹配");
        assertTrue(sendTps > 50, String.format("发送TPS应该大于50，实际: %.2f", sendTps));
        
        log.info("ActiveMQ真实批量消息测试完成：");
        log.info("- 批量大小: {}", batchSize);
        log.info("- 发送耗时: {}ms", sendTime);
        log.info("- 总耗时: {}ms", totalTime);
        log.info("- 发送TPS: {:.2f}", sendTps);
        log.info("- 整体TPS: {:.2f}", totalTps);
    }

    @Test
    void testPriorityMessages() throws Exception {
        int[] priorities = {1, 5, 9}; // 低、中、高优先级
        String[] priorityNames = {"低优先级", "中优先级", "高优先级"};
        int messagesPerPriority = 10;
        
        CountDownLatch priorityLatch = new CountDownLatch(priorities.length * messagesPerPriority);
        AtomicInteger priorityReceivedCount = new AtomicInteger(0);
        
        // 设置消息监听器
        consumer.setMessageListener(message -> {
            try {
                int priority = message.getJMSPriority();
                int count = priorityReceivedCount.incrementAndGet();
                log.debug("收到ActiveMQ优先级{}消息 {}", priority, count);
                priorityLatch.countDown();
            } catch (Exception e) {
                log.error("处理ActiveMQ优先级消息失败", e);
            }
        });
        
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
                
                TextMessage message = session.createTextMessage();
                String messageContent = String.format(
                    "{\"message\":\"%s\",\"priority\":%d,\"priorityName\":\"%s\",\"index\":%d}",
                    event.getMessage(), priority, priorityName, i
                );
                message.setText(messageContent);
                message.setStringProperty("tag", event.getTag());
                message.setJMSPriority(priority);
                
                producer.send(message);
            }
        }
        
        long prioritySendTime = System.currentTimeMillis() - priorityStartTime;
        
        // 等待所有优先级消息接收完成
        assertTrue(priorityLatch.await(30, TimeUnit.SECONDS), 
            "ActiveMQ优先级消息接收超时");
        
        long priorityTotalTime = System.currentTimeMillis() - priorityStartTime;
        int totalMessages = priorities.length * messagesPerPriority;
        double priorityTps = (double) totalMessages / (priorityTotalTime / 1000.0);
        
        assertEquals(totalMessages, priorityReceivedCount.get(), "优先级消息接收数量不匹配");
        
        log.info("ActiveMQ优先级消息测试完成：");
        log.info("- 优先级级别: {}", java.util.Arrays.toString(priorities));
        log.info("- 每个优先级消息数: {}", messagesPerPriority);
        log.info("- 总消息数: {}", totalMessages);
        log.info("- 发送耗时: {}ms", prioritySendTime);
        log.info("- 总耗时: {}ms", priorityTotalTime);
        log.info("- TPS: {:.2f}", priorityTps);
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
        log.info("创建单播消费者1");
        unicastConsumer1.setMessageListener(message -> {
            try {
                log.info("单播消费者1收到消息: {}", ((TextMessage) message).getText());
                consumer1Count.incrementAndGet();
                totalReceivedCount.incrementAndGet();
                latch.countDown();
            } catch (Exception e) {
                log.error("消费者1处理消息失败", e);
            }
        });
        
        // 创建第二个消费者（ActiveMQ中同一个队列的多个消费者会负载均衡）
        MessageConsumer unicastConsumer2 = session.createConsumer(unicastDestination);
        log.info("创建单播消费者2");
        unicastConsumer2.setMessageListener(message -> {
            try {
                log.info("单播消费者2收到消息: {}", ((TextMessage) message).getText());
                consumer2Count.incrementAndGet();
                totalReceivedCount.incrementAndGet();
                latch.countDown();
            } catch (Exception e) {
                log.error("消费者2处理消息失败", e);
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
        
        log.info("发送单播消息");
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
        
        log.info("ActiveMQ单播模式验证完成 - 消费者1收到: {}, 消费者2收到: {}, 总计: {}", 
            consumer1Count.get(), consumer2Count.get(), totalReceivedCount.get());
        
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
            
            log.info("ActiveMQ事务消息发送并提交成功");
        } catch (Exception e) {
            log.error("ActiveMQ事务消息发送失败，回滚事务", e);
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
        
        log.info("ActiveMQ真实事务消息测试完成");
    }

    @Test
    void testConnectionResilience() throws Exception {
        CountDownLatch resilienceLatch = new CountDownLatch(2);
        AtomicInteger resilienceReceivedCount = new AtomicInteger(0);
        
        // 设置弹性测试消息监听器
        consumer.setMessageListener(message -> {
            try {
                int count = resilienceReceivedCount.incrementAndGet();
                System.out.println("收到ActiveMQ弹性测试消息 " + count + ": " + ((TextMessage) message).getText());
                resilienceLatch.countDown();
            } catch (Exception e) {
                System.err.println("处理ActiveMQ弹性测试消息失败: " + e.getMessage());
            }
        });
        
        // 发送第一条消息
        TestEvent event1 = new TestEvent();
        event1.setMessage("ActiveMQ连接弹性测试消息1");
        event1.setTimestamp(System.currentTimeMillis());
        event1.setOrderId("ORDER-REAL-RESILIENCE-001");
        
        TextMessage message1 = session.createTextMessage();
        message1.setText(event1.getMessage());
        message1.setStringProperty("orderId", event1.getOrderId());
        
        producer.send(message1);
        
        // 模拟短暂断开连接
        log.info("模拟ActiveMQ连接断开...");
        connection.close();
        
        Thread.sleep(2000);
        
        // 重新连接
        log.info("ActiveMQ重新连接...");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKER_URL);
        connectionFactory.setTrustAllPackages(true);
        
        connection = connectionFactory.createConnection();
        connection.start();
        
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(QUEUE_NAME);
        producer = session.createProducer(destination);
        consumer = session.createConsumer(destination);
        
        // 重新设置消息监听器
        consumer.setMessageListener(message -> {
            try {
                int count = resilienceReceivedCount.incrementAndGet();
                System.out.println("收到ActiveMQ弹性测试消息 " + count + ": " + ((TextMessage) message).getText());
                resilienceLatch.countDown();
            } catch (Exception e) {
                System.err.println("处理ActiveMQ弹性测试消息失败: " + e.getMessage());
            }
        });
        
        // 发送第二条消息
        TestEvent event2 = new TestEvent();
        event2.setMessage("ActiveMQ连接弹性测试消息2");
        event2.setTimestamp(System.currentTimeMillis());
        event2.setOrderId("ORDER-REAL-RESILIENCE-002");
        
        TextMessage message2 = session.createTextMessage();
        message2.setText(event2.getMessage());
        message2.setStringProperty("orderId", event2.getOrderId());
        
        producer.send(message2);
        
        assertTrue(resilienceLatch.await(10, TimeUnit.SECONDS), 
            "ActiveMQ弹性测试消息接收超时");
        assertEquals(2, resilienceReceivedCount.get(), "应该收到2条弹性测试消息");
        
        log.info("ActiveMQ真实连接弹性测试完成");
    }



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
        log.info("ActiveMQ连接已关闭");
    }
}