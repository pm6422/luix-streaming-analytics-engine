package com.luixtech.frauddetection.simulator.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.framework.exception.DataNotFoundException;
import com.luixtech.frauddetection.simulator.domain.Rule;
import com.luixtech.frauddetection.simulator.model.RulePayload;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.services.FlinkRulesService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
class RuleController {

    private static final ObjectMapper      OBJECT_MAPPER = new ObjectMapper();
    private final        RuleRepository    repository;
    private final        FlinkRulesService flinkRulesService;

    @GetMapping("/rules")
    List<Rule> all() {
        return repository.findAll();
    }

    @PostMapping("/rules")
    Rule newRule(@RequestBody Rule newRule) throws IOException {
        Rule savedRule = repository.save(newRule);
        Integer id = savedRule.getId();
        RulePayload payload = OBJECT_MAPPER.readValue(savedRule.getRulePayload(), RulePayload.class);
        payload.setRuleId(id);
        String payloadJson = OBJECT_MAPPER.writeValueAsString(payload);
        savedRule.setRulePayload(payloadJson);
        Rule result = repository.save(savedRule);
        flinkRulesService.addRule(result);
        return result;
    }

    @GetMapping("/rules/pushToFlink")
    void pushToFlink() {
        List<Rule> rules = repository.findAll();
        for (Rule rule : rules) {
            flinkRulesService.addRule(rule);
        }
    }

    @GetMapping("/rules/{id}")
    Rule one(@PathVariable Integer id) {
        return repository.findById(id).orElseThrow(() -> new DataNotFoundException(id.toString()));
    }

    @DeleteMapping("/rules/{id}")
    void deleteRule(@PathVariable Integer id) throws JsonProcessingException {
        repository.deleteById(id);
        flinkRulesService.deleteRule(id);
    }

    @DeleteMapping("/rules")
    void deleteAllRules() throws JsonProcessingException {
        List<Rule> rules = repository.findAll();
        for (Rule rule : rules) {
            repository.deleteById(rule.getId());
            flinkRulesService.deleteRule(rule.getId());
        }
    }
}
