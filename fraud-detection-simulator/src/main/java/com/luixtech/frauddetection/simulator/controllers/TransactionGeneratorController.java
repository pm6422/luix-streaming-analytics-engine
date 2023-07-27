package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.generator.TransactionsGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@Slf4j
public class TransactionGeneratorController {

    private static final ExecutorService               EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    @Resource
    private              TransactionsGenerator         transactionsGenerator;
    @Resource
    private              KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @Resource
    private              ApplicationProperties         applicationProperties;

    private boolean generatingTransactions   = false;
    private boolean listenerContainerRunning = true;

    @GetMapping("/api/transaction-generator/start")
    public void startTransactionsGeneration() {
        if (!generatingTransactions) {
            EXECUTOR_SERVICE.submit(transactionsGenerator);
            generatingTransactions = true;
        }
    }

    @GetMapping("/api/transaction-generator/stop")
    public void stopTransactionsGeneration() {
        transactionsGenerator.cancel();
        generatingTransactions = false;
        log.info("{}", "stopTransactionsGeneration called");
    }

    @GetMapping("/api/transaction-generator/speed/{speed}")
    public void setGeneratorSpeed(@PathVariable Long speed) {
        log.info("Generator speed change request: " + speed);
        if (speed <= 0) {
            transactionsGenerator.cancel();
            generatingTransactions = false;
            return;
        } else {
            startTransactionsGeneration();
        }

        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry
                .getListenerContainer(applicationProperties.getKafka().getListeners().getTransactions().getId());
        if (speed > applicationProperties.getTransaction().getMaxTransactionSpeed()) {
            listenerContainer.stop();
            listenerContainerRunning = false;
        } else if (!listenerContainerRunning) {
            listenerContainer.start();
        }

        if (transactionsGenerator != null) {
            transactionsGenerator.adjustMaxRecordsPerSecond(speed);
        }
    }
}
