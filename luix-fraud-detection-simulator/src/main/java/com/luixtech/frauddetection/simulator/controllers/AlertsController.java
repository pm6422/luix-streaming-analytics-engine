package com.luixtech.frauddetection.simulator.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.frauddetection.common.alert.Alert;
import com.luixtech.frauddetection.common.input.InputRecord;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import com.luixtech.frauddetection.simulator.domain.DetectorRule;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaInputProducer;
import com.luixtech.frauddetection.simulator.repository.DetectorRuleRepository;
import com.luixtech.utilities.exception.DataNotFoundException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
@Slf4j
public class AlertsController {

    private static final ObjectMapper           OBJECT_MAPPER = new ObjectMapper();
    private final        DetectorRuleRepository detectorRuleRepository;
    private final        KafkaInputProducer     kafkaInputProducer;
    //    private final        SimpMessagingTemplate    simpSender;
    private final        ApplicationProperties  applicationProperties;

    @GetMapping("/alerts/mock")
    public Alert mockAlert(@RequestParam(value = "ruleId") String ruleId) throws JsonProcessingException {
        DetectorRule detectorRule = detectorRuleRepository.findById(ruleId).orElseThrow(() -> new DataNotFoundException(ruleId.toString()));
        InputRecord triggeringEvent = kafkaInputProducer.getLastInputRecord();
        if (triggeringEvent == null) {
            log.warn("No input record found, please start generating input records first");
            return null;
        }
        Alert alert = new Alert(detectorRule.getId(), detectorRule.toRuleCommand().getRule(), StringUtils.EMPTY, triggeringEvent);
        String result = OBJECT_MAPPER.writeValueAsString(alert);
        // Push to websocket queue
//        simpSender.convertAndSend(applicationProperties.getWebSocket().getTopic().getAlert(), result);
        return alert;
    }
}
