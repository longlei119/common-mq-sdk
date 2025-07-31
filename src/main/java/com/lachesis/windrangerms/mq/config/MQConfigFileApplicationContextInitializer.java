package com.lachesis.windrangerms.mq.config;

import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * MQ配置文件应用上下文初始化器
 * 负责在Spring应用上下文初始化时加载application-mq.yml配置文件
 */
public class MQConfigFileApplicationContextInitializer 
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        ConfigurableEnvironment environment = applicationContext.getEnvironment();
        try {
            Resource resource = new ClassPathResource("application-mq.yml");
            if (resource.exists()) {
                YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
                for (PropertySource<?> ps : loader.load("application-mq", resource)) {
                    environment.getPropertySources().addLast(ps);
                }
                System.out.println("✅ 已加载 application-mq.yml");
            } else {
                System.out.println("⚠️ application-mq.yml 配置文件不存在");
            }
        } catch (Exception e) {
            System.err.println("❌ 加载 application-mq.yml 配置文件失败:");
            e.printStackTrace();
        }
    }
}