package com.luixtech.frauddetection.simulator.config;

import com.luixtech.frauddetection.simulator.generator.TransactionsGenerator;
import com.luixtech.frauddetection.simulator.services.KafkaTransactionProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransactionGeneratorConfiguration {
    @Bean
    public TransactionsGenerator transactionsGenerator(KafkaTransactionProducer transactionsPusher) {
        return new TransactionsGenerator(transactionsPusher, 1);
    }
}
