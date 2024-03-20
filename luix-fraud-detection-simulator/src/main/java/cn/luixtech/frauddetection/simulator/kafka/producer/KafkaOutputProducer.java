package cn.luixtech.frauddetection.simulator.kafka.producer;

import cn.luixtech.cae.common.output.Output;
import cn.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Service
@AllArgsConstructor
@Slf4j
@Deprecated
public class KafkaOutputProducer implements Consumer<Output> {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ApplicationProperties         applicationProperties;

    @Override
    public void accept(Output output) {
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getOutput(), output);
        log.warn("Pushed output with content {}", output);
    }
}
