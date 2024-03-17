package com.luixtech.frauddetection.simulator.kafka.producer;

import com.luixtech.frauddetection.common.transaction.Transaction;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.function.Consumer;

@Service
@Slf4j
public class KafkaTransactionProducer implements Consumer<Transaction> {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Resource
    private ApplicationProperties         applicationProperties;
    @Getter
    private Transaction                   lastTransaction;

    @Override
    public void accept(Transaction transaction) {
        lastTransaction = transaction;
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getTransaction(), transaction);
        log.debug("Pushed transaction with content {}", transaction);
    }
}
