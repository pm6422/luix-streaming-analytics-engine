package cn.luixtech.frauddetection.simulator.kafka.producer;

import cn.luixtech.dae.common.rule.RuleGroup;
import cn.luixtech.dae.common.command.Command;
import cn.luixtech.dae.common.rule.RuleCommand;
import cn.luixtech.frauddetection.simulator.config.ApplicationProperties;
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
        ruleCommand.setCommand(Command.DELETE);
        RuleGroup ruleGroup = new RuleGroup();
        ruleGroup.setId(ruleId);
        ruleCommand.setRuleGroup(ruleGroup);
        kafkaTemplate.send(applicationProperties.getKafka().getTopic().getRule(), ruleCommand);
        log.info("Pushed deleting rule with ID {}", ruleId);
    }
}
