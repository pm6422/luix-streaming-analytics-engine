package com.luixtech.frauddetection.simulator.services;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.rule.RuleState;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
@Slf4j
public class KafkaRuleProducer {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Resource
    private ApplicationProperties         applicationProperties;

    public void addRule(Rule rule) {
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRule(), rule);
        log.info("Pushed adding rule with content {}", rule);
    }

    public void deleteRule(int ruleId) {
        Rule rule = new Rule();
        rule.setRuleId(ruleId);
        rule.setRuleState(RuleState.DELETE);
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRule(), rule);
        log.info("Pushed deleting rule with ID {}", ruleId);
    }
}
