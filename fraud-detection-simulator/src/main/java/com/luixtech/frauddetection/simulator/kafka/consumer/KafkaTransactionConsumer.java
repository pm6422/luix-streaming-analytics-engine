package com.luixtech.frauddetection.simulator.kafka.consumer;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@AllArgsConstructor
@Slf4j
public class KafkaTransactionConsumer implements ConsumerSeekAware {

    private final SimpMessagingTemplate         simpMessagingTemplate;
    private final ApplicationProperties         applicationProperties;
    private final KafkaListenerEndpointRegistry listenerEndpointRegistry;

    @KafkaListener(id = "${application.kafka.listener.transaction}", topics = "${application.kafka.topic.transaction}")
    public void consumeTransactions(@Payload String message) {
        log.debug("Received transaction {}", message);
        // Send to websocket
        simpMessagingTemplate.convertAndSend(applicationProperties.getWebSocket().getTopic().getTransaction(), message);
    }

    public void start() {
        MessageListenerContainer transactionConsumerListener = listenerEndpointRegistry
                .getListenerContainer(applicationProperties.getKafka().getListener().getTransaction());
        transactionConsumerListener.start();
    }

    public void stop() {
        MessageListenerContainer transactionConsumerListener = listenerEndpointRegistry
                .getListenerContainer(applicationProperties.getKafka().getListener().getTransaction());
        transactionConsumerListener.stop();
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
