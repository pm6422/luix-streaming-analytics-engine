package com.luixtech.frauddetection.simulator.config;

import com.luixtech.frauddetection.simulator.generator.TransactionsGenerator;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaTransactionProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransactionGeneratorConfiguration {
    @Bean
    public TransactionsGenerator transactionsGenerator(KafkaTransactionProducer transactionProducer) {
        return new TransactionsGenerator(transactionProducer, 1);
    }
}
