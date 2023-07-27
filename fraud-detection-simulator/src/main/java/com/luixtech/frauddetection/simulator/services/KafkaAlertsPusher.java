package com.luixtech.frauddetection.simulator.services;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.model.Alert;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Service
@AllArgsConstructor
@Slf4j
public class KafkaAlertsPusher implements Consumer<Alert> {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ApplicationProperties         applicationProperties;

    @Override
    public void accept(Alert alert) {
        log.info("{}", alert);
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getAlerts(), alert);
    }
}
