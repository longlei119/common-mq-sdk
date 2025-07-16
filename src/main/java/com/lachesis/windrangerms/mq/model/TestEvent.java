package com.lachesis.windrangerms.mq.model;

import com.lachesis.windrangerms.mq.enums.MQTypeEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TestEvent extends MQEvent {
    private String message;
    private String eventType = "TEST_EVENT";

    public TestEvent(String message) {
        this.message = message;
    }

    @Override
    public String getTopic() {
        return "TestTopic";
    }

    @Override
    public String getTag() {
        return "TestTag";
    }
}