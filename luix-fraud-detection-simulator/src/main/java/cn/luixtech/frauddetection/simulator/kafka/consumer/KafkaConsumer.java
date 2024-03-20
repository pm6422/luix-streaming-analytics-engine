package cn.luixtech.frauddetection.simulator.kafka.consumer;

import cn.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@Slf4j
public class KafkaConsumer {

//    private final SimpMessagingTemplate simpMessagingTemplate;
    private final ApplicationProperties applicationProperties;

    @KafkaListener(topics = "${application.kafka.topic.output}", groupId = "outputConsumeGrp")
    public void templateOutputs(@Payload String message) {
        log.warn("Found output {}", message);
        // Send to websocket
//        simpMessagingTemplate.convertAndSend(applicationProperties.getWebSocket().getTopic().getOutput(), message);
    }

    @KafkaListener(topics = "${application.kafka.topic.latency}", groupId = "latencyConsumeGrp")
    public void templateLatency(@Payload String message) {
        log.warn("Found latency {}ms", message);
        // Send to websocket
//        simpMessagingTemplate.convertAndSend(applicationProperties.getWebSocket().getTopic().getLatency(), message);
    }
}
