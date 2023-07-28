package com.luixtech.frauddetection.simulator.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.luixtech.framework.exception.DataNotFoundException;
import com.luixtech.frauddetection.simulator.domain.RulePayload;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.services.FlinkRulesService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
class RuleController {

    private final RuleRepository    repository;
    private final FlinkRulesService flinkRulesService;

    @GetMapping("/rules")
    List<RulePayload> all() {
        return repository.findAll();
    }

    @PostMapping("/rules")
    RulePayload newRule(@RequestBody RulePayload newRulePayload) {
        RulePayload savedRulePayload = repository.save(newRulePayload);
        flinkRulesService.addRule(savedRulePayload.toRule());
        return savedRulePayload;
    }

    @GetMapping("/rules/{id}")
    RulePayload one(@PathVariable Integer id) {
        return repository.findById(id).orElseThrow(() -> new DataNotFoundException(id.toString()));
    }

    @DeleteMapping("/rules/{id}")
    void deleteRule(@PathVariable Integer id) throws JsonProcessingException {
        repository.deleteById(id);
        flinkRulesService.deleteRule(id);
    }

    @DeleteMapping("/rules")
    void deleteAllRules() throws JsonProcessingException {
        List<RulePayload> rulePayloads = repository.findAll();
        for (RulePayload rulePayload : rulePayloads) {
            repository.deleteById(rulePayload.getId());
            flinkRulesService.deleteRule(rulePayload.getId());
        }
    }

    @GetMapping("/rules/push-to-flink")
    void pushToFlink() {
        List<RulePayload> rulePayloads = repository.findAll();
        for (RulePayload rulePayload : rulePayloads) {
            flinkRulesService.addRule(rulePayload.toRule());
        }
    }
}
