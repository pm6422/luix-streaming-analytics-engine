package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.framework.exception.DataNotFoundException;
import com.luixtech.frauddetection.simulator.domain.RulePayload;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.services.KafkaRulePusher;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
class RuleController {

    private final RuleRepository  ruleRepository;
    private final KafkaRulePusher kafkaRulePusher;

    @GetMapping("/rules")
    List<RulePayload> find() {
        return ruleRepository.findAll();
    }

    @PostMapping("/rules")
    RulePayload save(@RequestBody RulePayload newRulePayload) {
        RulePayload savedRulePayload = ruleRepository.save(newRulePayload);
        kafkaRulePusher.addRule(savedRulePayload.toRule());
        return savedRulePayload;
    }

    @GetMapping("/rules/{id}")
    RulePayload find(@PathVariable Integer id) {
        return ruleRepository.findById(id).orElseThrow(() -> new DataNotFoundException(id.toString()));
    }

    @DeleteMapping("/rules/{id}")
    void delete(@PathVariable Integer id) {
        ruleRepository.deleteById(id);
        kafkaRulePusher.deleteRule(id);
    }

    @DeleteMapping("/rules")
    void deleteAll() {
        List<RulePayload> rulePayloads = ruleRepository.findAll();
        for (RulePayload rulePayload : rulePayloads) {
            ruleRepository.deleteById(rulePayload.getId());
            kafkaRulePusher.deleteRule(rulePayload.getId());
        }
    }
}
