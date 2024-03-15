package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.frauddetection.common.command.Control;
import com.luixtech.frauddetection.common.pojo.RuleCommand;
import com.luixtech.frauddetection.simulator.domain.DetectorRule;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaRuleProducer;
import com.luixtech.frauddetection.simulator.repository.DetectorRuleRepository;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@AllArgsConstructor
public class SyncRuleController {

    private final DetectorRuleRepository detectorRuleRepository;
    private final KafkaRuleProducer      kafkaRuleProducer;

    @GetMapping("/api/rules/sync-all")
    public void syncAllRules() {
        List<DetectorRule> detectorRules = detectorRuleRepository.findAll();
        for (DetectorRule detectorRule : detectorRules) {
            kafkaRuleProducer.addRule(detectorRule.toRuleCommand());
        }
    }

    @GetMapping("/api/rules/delete-all")
    public void deleteAllRules() {
        RuleCommand ruleCommand = new RuleCommand();
        ruleCommand.setControl(Control.DELETE_ALL);
        kafkaRuleProducer.addRule(ruleCommand);
    }
}
