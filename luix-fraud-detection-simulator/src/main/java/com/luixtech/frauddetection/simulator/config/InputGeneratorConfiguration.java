package com.luixtech.frauddetection.simulator.config;

import com.luixtech.frauddetection.simulator.generator.InputGenerator;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaInputProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InputGeneratorConfiguration {
    @Bean
    public InputGenerator inputGenerator(KafkaInputProducer inputProducer) {
        return new InputGenerator(inputProducer, 1);
    }
}
