package com.luixtech.frauddetection.simulator.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.rule.ControlType;
import com.luixtech.frauddetection.common.rule.RuleState;
import com.luixtech.frauddetection.simulator.domain.RulePayload;
import com.luixtech.frauddetection.simulator.services.FlinkRulesService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
public class FlinkController {

    private final FlinkRulesService flinkRulesService;

    @GetMapping("/flink/export-current-rules")
    void exportCurrentRules() {
        RulePayload command = createControlCommand(ControlType.EXPORT_CURRENT_RULES);
        flinkRulesService.addRule(command.toRule());
    }

    @GetMapping("/flink/clear-state")
    void clearState() {
        RulePayload command = createControlCommand(ControlType.CLEAR_STATE_ALL);
        flinkRulesService.addRule(command.toRule());
    }

    private RulePayload createControlCommand(ControlType clearStateAll) {
        Rule rule = new Rule();
        rule.setRuleState(RuleState.CONTROL);
        rule.setControlType(clearStateAll);
        return RulePayload.fromRule(rule);
    }
}
