package cn.luixtech.frauddetection.simulator.kafka.producer;

import cn.luixtech.dae.common.input.Input;
import cn.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.function.Consumer;

@Service
@Slf4j
public class KafkaInputProducer implements Consumer<Input> {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Resource
    private ApplicationProperties         applicationProperties;
    @Getter
    private Input                         lastInput;

    @Override
    public void accept(Input input) {
        lastInput = input;
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getInput(), input);
        log.debug("Pushed input with content {}", input);
    }
}
