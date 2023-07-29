package com.luixtech.frauddetection.simulator.services;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@Slf4j
public class KafkaConsumer {

    private final        SimpMessagingTemplate simpTemplate;
    private final        ApplicationProperties applicationProperties;

    @KafkaListener(topics = "${application.kafka.topic.alert}", groupId = "alerts")
    public void templateAlerts(@Payload String message) {
        log.warn("Detected alert {}", message);
        // Send to websocket
        simpTemplate.convertAndSend(applicationProperties.getWebSocket().getTopic().getAlert(), message);
    }

    @KafkaListener(topics = "${application.kafka.topic.latency}", groupId = "latency")
    public void templateLatency(@Payload String message) {
        log.warn("Found latency {}ms", message);
        // Send to websocket
        simpTemplate.convertAndSend(applicationProperties.getWebSocket().getTopic().getLatency(), message);
    }
}
