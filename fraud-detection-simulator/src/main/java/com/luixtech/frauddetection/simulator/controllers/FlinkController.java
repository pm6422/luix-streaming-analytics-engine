package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.rule.ControlType;
import com.luixtech.frauddetection.common.rule.RuleState;
import com.luixtech.frauddetection.simulator.domain.RulePayload;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.services.KafkaRulePusher;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
public class FlinkController {

    private final RuleRepository  ruleRepository;
    private final KafkaRulePusher kafkaRulePusher;

    @GetMapping("/flink/sync-rules")
    void syncRules() {
        List<RulePayload> rulePayloads = ruleRepository.findAll();
        for (RulePayload rulePayload : rulePayloads) {
            kafkaRulePusher.addRule(rulePayload.toRule());
        }
    }

    @GetMapping("/flink/export-current-rules")
    void exportCurrentRules() {
        RulePayload command = createControlCommand(ControlType.EXPORT_CURRENT_RULES);
        kafkaRulePusher.addRule(command.toRule());
    }

    @GetMapping("/flink/clear-state")
    void clearState() {
        RulePayload command = createControlCommand(ControlType.CLEAR_STATE_ALL);
        kafkaRulePusher.addRule(command.toRule());
    }

    private RulePayload createControlCommand(ControlType clearStateAll) {
        Rule rule = new Rule();
        rule.setRuleState(RuleState.CONTROL);
        rule.setControlType(clearStateAll);
        return RulePayload.fromRule(rule);
    }
}
