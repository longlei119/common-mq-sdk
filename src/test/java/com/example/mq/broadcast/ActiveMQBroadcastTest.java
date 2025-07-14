package com.example.mq.broadcast;

import com.example.mq.model.MQEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;

import javax.jms.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ActiveMQ广播模式测试
 */
@Slf4j
@SpringBootTest
@ConditionalOnProperty(name = "mq.activemq.enabled", havingValue = "true")
public class ActiveMQBroadcastTest {

    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";
    private static final String QUEUE_NAME = "test.queue";
    private static final String TAG = "test";

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKER_URL);
        connectionFactory.setTrustAllPackages(true);
        
        connection = connectionFactory.createConnection();
        connection.start();
        
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        log.info("ActiveMQ连接已建立");
    }

    @Test
    void testActiveMQUnicastVsBroadcast() throws Exception {
        // 单播模式测试
        String unicastQueueName = QUEUE_NAME + ".unicast";
        Destination unicastDestination = session.createQueue(unicastQueueName);
        MessageProducer unicastProducer = session.createProducer(unicastDestination);
        
        AtomicInteger unicastCount = new AtomicInteger(0);
        CountDownLatch unicastLatch = new CountDownLatch(1); // ActiveMQ单播模式应该只有一个消费者收到消息
        
        // 创建两个单播消费者（ActiveMQ中同一个队列的多个消费者会负载均衡）
        MessageConsumer unicastConsumer1 = session.createConsumer(unicastDestination);
        MessageConsumer unicastConsumer2 = session.createConsumer(unicastDestination);
        
        log.info("开始订阅单播消费者1");
        unicastConsumer1.setMessageListener(message -> {
            try {
                System.out.println("单播消费者1收到消息: " + ((TextMessage) message).getText());
                unicastCount.incrementAndGet();
                unicastLatch.countDown();
            } catch (Exception e) {
                System.err.println("单播消费者1处理消息失败: " + e.getMessage());
            }
        });
        
        log.info("开始订阅单播消费者2");
        unicastConsumer2.setMessageListener(message -> {
            try {
                System.out.println("单播消费者2收到消息: " + ((TextMessage) message).getText());
                unicastCount.incrementAndGet();
                unicastLatch.countDown();
            } catch (Exception e) {
                System.err.println("单播消费者2处理消息失败: " + e.getMessage());
            }
        });
        
        // 等待订阅生效
        Thread.sleep(1000);
        
        // 发送单播消息
        TestEvent unicastEvent = new TestEvent();
        unicastEvent.setMessage("ActiveMQ单播测试消息");
        unicastEvent.setTimestamp(System.currentTimeMillis());
        unicastEvent.setOrderId("ORDER-UNICAST-001");
        unicastEvent.setAmount(100.0);
        
        TextMessage unicastMessage = session.createTextMessage();
        unicastMessage.setText(unicastEvent.getMessage());
        unicastMessage.setStringProperty("orderId", unicastEvent.getOrderId());
        
        log.info("发送单播消息");
        unicastProducer.send(unicastMessage);
        
        // 等待消息处理
        assertTrue(unicastLatch.await(10, TimeUnit.SECONDS), "单播消息接收超时");
        
        // 验证单播模式：ActiveMQ队列模式下，多个消费者会负载均衡，但只有一个消费者收到消息
        assertEquals(1, unicastCount.get(), "ActiveMQ单播模式应该只有一个消费者收到消息，实际收到: " + unicastCount.get());
        
        log.info("ActiveMQ单播模式测试完成，收到消息数: {}", unicastCount.get());
        
        // 广播模式测试（使用Topic）
        String broadcastTopicName = "test.broadcast.topic";
        Destination broadcastDestination = session.createTopic(broadcastTopicName);
        MessageProducer broadcastProducer = session.createProducer(broadcastDestination);
        
        AtomicInteger broadcastCount = new AtomicInteger(0);
        CountDownLatch broadcastLatch = new CountDownLatch(2); // 广播模式期望2个消费者都收到消息
        
        // 创建两个广播消费者
        MessageConsumer broadcastConsumer1 = session.createConsumer(broadcastDestination);
        MessageConsumer broadcastConsumer2 = session.createConsumer(broadcastDestination);
        
        log.info("开始订阅广播消费者1");
        broadcastConsumer1.setMessageListener(message -> {
            try {
                System.out.println("广播消费者1收到消息: " + ((TextMessage) message).getText());
                broadcastCount.incrementAndGet();
                broadcastLatch.countDown();
            } catch (Exception e) {
                System.err.println("广播消费者1处理消息失败: " + e.getMessage());
            }
        });
        
        log.info("开始订阅广播消费者2");
        broadcastConsumer2.setMessageListener(message -> {
            try {
                System.out.println("广播消费者2收到消息: " + ((TextMessage) message).getText());
                broadcastCount.incrementAndGet();
                broadcastLatch.countDown();
            } catch (Exception e) {
                System.err.println("广播消费者2处理消息失败: " + e.getMessage());
            }
        });
        
        // 等待广播订阅生效
        Thread.sleep(1000);
        
        // 发送广播消息
        TestEvent broadcastEvent = new TestEvent();
        broadcastEvent.setMessage("ActiveMQ广播测试消息");
        broadcastEvent.setTimestamp(System.currentTimeMillis());
        broadcastEvent.setOrderId("ORDER-BROADCAST-001");
        broadcastEvent.setAmount(200.0);
        
        TextMessage broadcastMessage = session.createTextMessage();
        broadcastMessage.setText(broadcastEvent.getMessage());
        broadcastMessage.setStringProperty("orderId", broadcastEvent.getOrderId());
        
        log.info("发送广播消息");
        broadcastProducer.send(broadcastMessage);
        
        // 等待广播消息处理
        assertTrue(broadcastLatch.await(10, TimeUnit.SECONDS), "广播消息接收超时");
        
        // 验证广播模式：所有消费者都应该收到消息
        assertEquals(2, broadcastCount.get(), "ActiveMQ广播模式所有消费者都应该收到消息，实际收到: " + broadcastCount.get());
        
        log.info("ActiveMQ广播模式测试完成，收到消息数: {}", broadcastCount.get());
        log.info("ActiveMQ单播vs广播测试完成");
        
        // 清理资源
        unicastConsumer1.close();
        unicastConsumer2.close();
        unicastProducer.close();
        broadcastConsumer1.close();
        broadcastConsumer2.close();
        broadcastProducer.close();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (session != null) {
            session.close();
        }
        if (connection != null) {
            connection.close();
        }
        log.info("ActiveMQ连接已关闭");
    }

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
}