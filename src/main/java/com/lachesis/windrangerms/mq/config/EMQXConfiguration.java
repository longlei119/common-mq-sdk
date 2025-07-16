package com.lachesis.windrangerms.mq.config;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
public class EMQXConfiguration {

    @Bean
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
}