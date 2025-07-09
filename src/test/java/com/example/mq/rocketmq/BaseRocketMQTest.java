package com.example.mq.rocketmq;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {
    "mq.rocketmq.name-server-addr=localhost:9876",
    "mq.rocketmq.producer-group=test-producer-group",
    "mq.rocketmq.consumer-group=test-consumer-group",
    "mq.rocketmq.send-msg-timeout=3000",
    "mq.rocketmq.consumer.thread-min=20",
    "mq.rocketmq.consumer.thread-max=64",
    "mq.rocketmq.consumer.batch-max-size=1",
    "mq.rocketmq.consumer.consume-timeout=15000"
})
public class BaseRocketMQTest {
}