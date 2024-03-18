package com.luixtech.frauddetection.simulator.config;

import com.luixtech.frauddetection.simulator.generator.InputRecordGenerator;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaInputProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InputRecordGeneratorConfiguration {
    @Bean
    public InputRecordGenerator inputRecordGenerator(KafkaInputProducer inputRecordProducer) {
        return new InputRecordGenerator(inputRecordProducer, 1);
    }
}
