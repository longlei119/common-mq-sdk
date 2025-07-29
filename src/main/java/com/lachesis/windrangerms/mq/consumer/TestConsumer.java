package com.lachesis.windrangerms.mq.consumer;

import com.lachesis.windrangerms.mq.factory.MQFactory;
import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "mq.rocketmq", name = {"enabled", "name-server-addr"}, havingValue = "true")
public class TestConsumer {

    @Autowired
    private MQFactory mqFactory;

    @PostConstruct
    public void init() {
        try {
            // 订阅消息并处理
            mqFactory.getConsumer(MQTypeEnum.ROCKET_MQ).subscribe(MQTypeEnum.ROCKET_MQ, "TestTopic", "TestTag", message -> {
                log.info("收到消息：{}", message);
                // 这里可以添加具体的业务处理逻辑
            });
            log.info("RocketMQ 消息订阅成功：TestTopic:TestTag");
        } catch (IllegalArgumentException e) {
            log.warn("RocketMQ 未配置或不可用，跳过消息订阅");
        } catch (Exception e) {
            log.error("RocketMQ 消息订阅失败", e);
        }
    }
}