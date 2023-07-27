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
public class KafkaTransactionsConsumerService implements ConsumerSeekAware {

    private final SimpMessagingTemplate simpTemplate;
    private final ApplicationProperties applicationProperties;

    @KafkaListener(id = "${application.kafka.listeners.transactions.id}", topics = "${application.kafka.topic.transactions}", groupId = "transactions")
    public void consumeTransactions(@Payload String message) {
        log.debug("{}", message);
        simpTemplate.convertAndSend(applicationProperties.getWebSocket().getTopic().getTransactions(), message);
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
