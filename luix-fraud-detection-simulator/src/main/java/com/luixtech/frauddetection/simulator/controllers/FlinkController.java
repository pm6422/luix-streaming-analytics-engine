package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.rule.RuleControl;
import com.luixtech.frauddetection.common.rule.RuleState;
import com.luixtech.frauddetection.simulator.domain.RulePayload;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaRuleProducer;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
public class FlinkController {

    private final RuleRepository    ruleRepository;
    private final KafkaRuleProducer kafkaRuleProducer;

    @GetMapping("/flink/sync-rules")
    void syncRules() {
        List<RulePayload> rulePayloads = ruleRepository.findAll();
        for (RulePayload rulePayload : rulePayloads) {
            kafkaRuleProducer.addRule(rulePayload.toRule());
        }
    }

    @GetMapping("/flink/clear-state")
    void clearState() {
        RulePayload command = createControlCommand(RuleControl.CLEAR_ALL_STATE);
        kafkaRuleProducer.addRule(command.toRule());
    }

    private RulePayload createControlCommand(RuleControl clearStateAll) {
        Rule rule = new Rule();
        rule.setRuleState(RuleState.CONTROL);
        rule.setRuleControl(clearStateAll);
        return RulePayload.fromRule(rule);
    }
}
