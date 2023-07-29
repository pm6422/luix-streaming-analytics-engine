package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.generator.TransactionsGenerator;
import com.luixtech.frauddetection.simulator.kafka.consumer.KafkaTransactionConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@Slf4j
public class TransactionGeneratorController {

    private static final ExecutorService          EXECUTOR_SERVICE         = Executors.newSingleThreadExecutor();
    @Resource
    private              TransactionsGenerator    transactionsGenerator;
    @Resource
    private              ApplicationProperties    applicationProperties;
    @Resource
    private              KafkaTransactionConsumer kafkaTransactionConsumer;
    private              boolean                  listenerContainerRunning = true;
    private static final AtomicBoolean            GENERATING               = new AtomicBoolean(false);

    @GetMapping("/api/transaction-generator/start")
    public void start() {
        if (GENERATING.compareAndSet(false, true)) {
            EXECUTOR_SERVICE.submit(transactionsGenerator);
        }
    }

    @GetMapping("/api/transaction-generator/stop")
    public void stop() {
        if (GENERATING.compareAndSet(true, false)) {
            transactionsGenerator.cancel();
        }
    }

    @GetMapping("/api/transaction-generator/speed/{speed}")
    public void setGeneratorSpeed(@PathVariable Long speed) {
        log.info("Generator speed change request: " + speed);
        if (speed <= 0) {
            transactionsGenerator.cancel();
            GENERATING.set(false);
            return;
        } else {
            start();
        }

        if (speed > applicationProperties.getTransaction().getMaxTransactionSpeed()) {
            kafkaTransactionConsumer.stop();
            listenerContainerRunning = false;
        } else if (!listenerContainerRunning) {
            kafkaTransactionConsumer.start();
        }

        if (transactionsGenerator != null) {
            transactionsGenerator.adjustMaxRecordsPerSecond(speed);
        }
    }
}
