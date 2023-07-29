package com.luixtech.frauddetection.simulator.services;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@AllArgsConstructor
@Slf4j
public class KafkaTransactionConsumer implements ConsumerSeekAware {

    private final SimpMessagingTemplate simpTemplate;
    private final ApplicationProperties applicationProperties;

    @KafkaListener(id = "${application.kafka.listener.transaction.id}", topics = "${application.kafka.topic.transaction}", groupId = "transactions")
    public void consumeTransactions(@Payload String message) {
        log.debug("Received transaction {}", message);
        // Send to websocket
        simpTemplate.convertAndSend(applicationProperties.getWebSocket().getTopic().getTransaction(), message);
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((t, o) -> callback.seekToEnd(t.topic(), t.partition()));
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    }
}
