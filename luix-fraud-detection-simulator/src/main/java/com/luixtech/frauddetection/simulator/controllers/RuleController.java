package com.luixtech.frauddetection.simulator.controllers;

import com.luixtech.framework.exception.DataNotFoundException;
import com.luixtech.frauddetection.simulator.domain.RulePayload;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaRuleProducer;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@AllArgsConstructor
class RuleController {

    private final RuleRepository    ruleRepository;
    private final KafkaRuleProducer kafkaRuleProducer;

    @GetMapping("/rules")
    List<RulePayload> find() {
        return ruleRepository.findAll();
    }

    @PostMapping("/rules")
    RulePayload createOrUpdate(@RequestBody RulePayload newRulePayload) {
        RulePayload savedRulePayload = ruleRepository.save(newRulePayload);
        kafkaRuleProducer.addRule(savedRulePayload.toRule());
        return savedRulePayload;
    }

    @GetMapping("/rules/{id}")
    RulePayload find(@PathVariable Integer id) {
        return ruleRepository.findById(id).orElseThrow(() -> new DataNotFoundException(id.toString()));
    }

    @DeleteMapping("/rules/{id}")
    void delete(@PathVariable Integer id) {
        ruleRepository.deleteById(id);
        kafkaRuleProducer.deleteRule(id);
    }

    @DeleteMapping("/rules")
    void deleteAll() {
        List<RulePayload> rulePayloads = ruleRepository.findAll();
        for (RulePayload rulePayload : rulePayloads) {
            ruleRepository.deleteById(rulePayload.getId());
            kafkaRuleProducer.deleteRule(rulePayload.getId());
        }
    }
}
