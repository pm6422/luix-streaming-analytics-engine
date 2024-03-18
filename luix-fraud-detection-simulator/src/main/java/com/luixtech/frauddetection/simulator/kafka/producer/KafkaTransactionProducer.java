package com.luixtech.frauddetection.simulator.kafka.producer;

import com.luixtech.frauddetection.common.input.InputRecord;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.function.Consumer;

@Service
@Slf4j
public class KafkaTransactionProducer implements Consumer<InputRecord> {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Resource
    private ApplicationProperties         applicationProperties;
    @Getter
    private InputRecord                   lastTransaction;

    @Override
    public void accept(InputRecord input) {
        lastTransaction = input;
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getTransaction(), input);
        log.debug("Pushed input with content {}", input);
    }
}
