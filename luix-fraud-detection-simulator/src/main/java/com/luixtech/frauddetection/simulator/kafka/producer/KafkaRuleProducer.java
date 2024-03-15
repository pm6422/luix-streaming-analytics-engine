package com.luixtech.frauddetection.simulator.kafka.producer;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.rule.RuleControl;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class KafkaRuleProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ApplicationProperties         applicationProperties;

    public void addRule(Rule rule) {
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRule(), rule);
        log.info("Pushed adding rule with content {}", rule);
    }

    public void deleteRule(int ruleId) {
        Rule rule = new Rule();
        rule.setRuleId(ruleId);
        rule.setRuleControl(RuleControl.DELETE);
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRule(), rule);
        log.info("Pushed deleting rule with ID {}", ruleId);
    }
}
