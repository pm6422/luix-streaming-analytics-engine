package cn.luixtech.frauddetection.simulator.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cn.luixtech.cae.common.input.Input;
import cn.luixtech.cae.common.output.Output;
import cn.luixtech.frauddetection.simulator.config.ApplicationProperties;
import cn.luixtech.frauddetection.simulator.domain.Detector;
import cn.luixtech.frauddetection.simulator.kafka.producer.KafkaInputProducer;
import cn.luixtech.frauddetection.simulator.repository.DetectorRepository;
import com.luixtech.utilities.exception.DataNotFoundException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
@Slf4j
public class OutputController {

    private static final ObjectMapper       OBJECT_MAPPER = new ObjectMapper();
    private final        DetectorRepository detectorRepository;
    private final        KafkaInputProducer kafkaInputProducer;
    //    private final        SimpMessagingTemplate    simpSender;
    private final        ApplicationProperties  applicationProperties;

    @GetMapping("/api/outputs/mock")
    public Output mockOutput(@RequestParam(value = "ruleId") String ruleId) throws JsonProcessingException {
        Detector detector = detectorRepository.findById(ruleId).orElseThrow(() -> new DataNotFoundException(ruleId.toString()));
        Input triggeringEvent = kafkaInputProducer.getLastInput();
        if (triggeringEvent == null) {
            log.warn("No input record found, please start generating input records first");
            return null;
        }
        Output output = new Output(detector.getId(), detector.toRuleCommand().getRuleGroup(), StringUtils.EMPTY, triggeringEvent);
        String result = OBJECT_MAPPER.writeValueAsString(output);
        // Push to websocket queue
//        simpSender.convertAndSend(applicationProperties.getWebSocket().getTopic().getOutput(), result);
        return output;
    }
}
