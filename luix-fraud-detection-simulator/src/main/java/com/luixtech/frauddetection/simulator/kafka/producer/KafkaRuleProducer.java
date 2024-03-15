package com.luixtech.frauddetection.simulator.kafka.producer;

import com.luixtech.frauddetection.common.pojo.Rule;
import com.luixtech.frauddetection.common.command.Control;
import com.luixtech.frauddetection.common.pojo.RuleCommand;
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

    public void addRule(RuleCommand ruleCommand) {
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRule(), ruleCommand);
        log.info("Pushed adding rule with content {}", ruleCommand);
    }

    public void deleteRule(String ruleId) {
        RuleCommand ruleCommand = new RuleCommand();
        ruleCommand.setControl(Control.DELETE);
        Rule rule = new Rule();
        rule.setId(ruleId);
        ruleCommand.setRule(rule);
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRule(), ruleCommand);
        log.info("Pushed deleting rule with ID {}", ruleId);
    }
}
