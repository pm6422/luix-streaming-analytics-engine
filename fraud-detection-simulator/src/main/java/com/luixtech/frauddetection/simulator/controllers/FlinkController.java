package com.luixtech.frauddetection.simulator.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.frauddetection.simulator.domain.Rule;
import com.luixtech.frauddetection.simulator.model.RulePayload;
import com.luixtech.frauddetection.simulator.model.RulePayload.ControlType;
import com.luixtech.frauddetection.simulator.model.RulePayload.RuleState;
import com.luixtech.frauddetection.simulator.services.FlinkRulesService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
public class FlinkController {

    private static final ObjectMapper      OBJECT_MAPPER = new ObjectMapper();
    private final        FlinkRulesService flinkRulesService;

    @GetMapping("/flink/sync-rules")
    void syncRules() throws JsonProcessingException {
        Rule command = createControlCommand(ControlType.EXPORT_RULES_CURRENT);
        flinkRulesService.addRule(command);
    }

    @GetMapping("/flink/clear-state")
    void clearState() throws JsonProcessingException {
        Rule command = createControlCommand(ControlType.CLEAR_STATE_ALL);
        flinkRulesService.addRule(command);
    }

    private Rule createControlCommand(ControlType clearStateAll) throws JsonProcessingException {
        RulePayload payload = new RulePayload();
        payload.setRuleState(RuleState.CONTROL);
        payload.setControlType(clearStateAll);
        Rule rule = new Rule();
        rule.setRulePayload(OBJECT_MAPPER.writeValueAsString(payload));
        return rule;
    }
}
