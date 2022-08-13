package com.mctoluene.microservice.demo.kafka.admin.config;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import com.mctoluene.microservice.demo.config.KafkaConfigData;

@EnableRetry
@Configuration
public class KafkaAdminConfig {

    private final KafkaConfigData kafkaConfgData;

    public KafkaAdminConfig(KafkaConfigData kafkaConfgData) {
        this.kafkaConfgData = kafkaConfgData;
    }

    @Bean
    public AdminClient adminClient() {
        return AdminClient
                .create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaConfgData.getBootstrapServers()));
    }
}
