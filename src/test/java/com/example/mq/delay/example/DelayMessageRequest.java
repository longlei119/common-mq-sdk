package com.example.mq.delay.example;

import java.util.Map;

/**
 * 延迟消息请求对象
 */
public class DelayMessageRequest {

    /**
     * 消息主题
     */
    private String topic;
    
    /**
     * 消息标签
     */
    private String tag;
    
    /**
     * 消息内容
     */
    private String body;
    
    /**
     * 消息队列类型
     * 可选值：ROCKET_MQ, ACTIVE_MQ, RABBIT_MQ, KAFKA
     */
    private String mqType;
    
    /**
     * 延迟毫秒数
     */
    private long delayMillis;
    
    /**
     * 消息属性
     */
    private Map<String, String> properties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public long getDelayMillis() {
        return delayMillis;
    }

    public void setDelayMillis(long delayMillis) {
        this.delayMillis = delayMillis;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}