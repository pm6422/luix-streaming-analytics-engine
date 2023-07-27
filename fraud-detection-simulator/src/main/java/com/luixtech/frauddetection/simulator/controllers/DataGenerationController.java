package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.datasource.DemoTransactionsGenerator;
import com.luixtech.frauddetection.simulator.datasource.TransactionsGenerator;
import com.luixtech.frauddetection.simulator.services.KafkaTransactionsPusher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@Slf4j
public class DataGenerationController {

    private static final ExecutorService               EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    private final        TransactionsGenerator         transactionsGenerator;
    private final        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final        ApplicationProperties         applicationProperties;

    private boolean generatingTransactions   = false;
    private boolean listenerContainerRunning = true;

    @Value("${kafka.listeners.transactions.id}")
    private String transactionListenerId;

    @Autowired
    public DataGenerationController(KafkaTransactionsPusher transactionsPusher,
                                    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
                                    ApplicationProperties applicationProperties) {
        this.transactionsGenerator = new DemoTransactionsGenerator(transactionsPusher, 1);
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.applicationProperties = applicationProperties;
    }

    @GetMapping("/api/startTransactionsGeneration")
    public void startTransactionsGeneration() throws Exception {
        log.info("{}", "startTransactionsGeneration called");
        generateTransactions();
    }

    private void generateTransactions() {
        if (!generatingTransactions) {
            EXECUTOR_SERVICE.submit(transactionsGenerator);
            generatingTransactions = true;
        }
    }

    @GetMapping("/api/stopTransactionsGeneration")
    public void stopTransactionsGeneration() {
        transactionsGenerator.cancel();
        generatingTransactions = false;
        log.info("{}", "stopTransactionsGeneration called");
    }

    @GetMapping("/api/generatorSpeed/{speed}")
    public void setGeneratorSpeed(@PathVariable Long speed) {
        log.info("Generator speed change request: " + speed);
        if (speed <= 0) {
            transactionsGenerator.cancel();
            generatingTransactions = false;
            return;
        } else {
            generateTransactions();
        }

        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(transactionListenerId);
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
