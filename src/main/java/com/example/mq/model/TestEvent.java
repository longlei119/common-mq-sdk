package com.example.mq.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TestEvent extends MQEvent {
    private String message;

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