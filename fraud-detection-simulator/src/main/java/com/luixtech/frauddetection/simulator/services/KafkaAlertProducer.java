package com.luixtech.frauddetection.simulator.services;

import com.luixtech.frauddetection.common.dto.Alert;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Service
@AllArgsConstructor
@Slf4j
@Deprecated
public class KafkaAlertProducer implements Consumer<Alert> {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ApplicationProperties         applicationProperties;

    @Override
    public void accept(Alert alert) {
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getAlerts(), alert);
        log.warn("Pushed alert with content {}", alert);
    }
}
