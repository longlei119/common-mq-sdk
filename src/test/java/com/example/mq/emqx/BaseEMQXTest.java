package com.example.mq.emqx;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {"mq.emqx.enabled=false"})
public class BaseEMQXTest {

    @Configuration
    static class EMQXTestConfig {

        @Bean
        @ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
        public MqttPahoClientFactory mqttClientFactory() {
            DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
            MqttConnectOptions options = new MqttConnectOptions();
            options.setServerURIs(new String[]{"tcp://localhost:1883"});
            options.setUserName("admin");
            options.setPassword("public".toCharArray());
            options.setCleanSession(true);
            options.setKeepAliveInterval(60);
            factory.setConnectionOptions(options);
            return factory;
        }

        @Bean
        @ConditionalOnProperty(prefix = "mq.emqx", name = "enabled", havingValue = "true")
        public MqttClient mqttClient(MqttPahoClientFactory mqttClientFactory) throws Exception {
            MqttClient client = (MqttClient) mqttClientFactory.getClientInstance("tcp://localhost:1883", "testClient");
            client.connect(mqttClientFactory.getConnectionOptions());
            return client;
        }
    }
}