package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.datasource.DemoTransactionsGenerator;
import com.luixtech.frauddetection.simulator.datasource.TransactionsGenerator;
import com.luixtech.frauddetection.simulator.services.KafkaTransactionsPusher;
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
public class DataGenerationController {

    private static final ExecutorService               EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    private              TransactionsGenerator         transactionsGenerator;
    @Resource
    private              KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @Resource
    private              ApplicationProperties         applicationProperties;

    private boolean generatingTransactions   = false;
    private boolean listenerContainerRunning = true;

    public DataGenerationController(KafkaTransactionsPusher transactionsPusher) {
        this.transactionsGenerator = new DemoTransactionsGenerator(transactionsPusher, 1);
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
