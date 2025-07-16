package com.lachesis.windrangerms.mq.example;

import com.lachesis.windrangerms.mq.config.MQConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class MQConfigPrintTest implements CommandLineRunner {
    @Autowired
    private MQConfig mqConfig;

    @Override
    public void run(String... args) {
        System.out.println("Loaded MQConfig: " + mqConfig);
    }
}