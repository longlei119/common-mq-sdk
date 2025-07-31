package com.lachesis.windrangerms.mq.config;

import com.lachesis.windrangerms.mq.delay.adapter.EMQXAdapter;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * EMQX消息队列自动配置类
 */
@Configuration
@ConditionalOnClass(name = "org.eclipse.paho.client.mqttv3.MqttClient")
public class EMQXAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean
    public MqttClient mqttClient(MQConfig mqConfig) throws MqttException {
        MQConfig.EMQXProperties emqxConfig = mqConfig.getEmqx();
        
        MqttClient mqttClient = new MqttClient(emqxConfig.getServerUri(), emqxConfig.getClientId());
        
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(emqxConfig.getUsername());
        options.setPassword(emqxConfig.getPassword().toCharArray());
        options.setCleanSession(emqxConfig.isCleanSession());
        options.setKeepAliveInterval(emqxConfig.getKeepAliveInterval());
        
        mqttClient.connect(options);
        
        return mqttClient;
    }

    @Bean
    @ConditionalOnBean(MqttClient.class)
    @ConditionalOnMissingBean
    public EMQXAdapter emqxAdapter(MqttClient mqttClient) {
        return new EMQXAdapter(mqttClient);
    }
}