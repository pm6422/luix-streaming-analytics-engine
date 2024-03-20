package cn.luixtech.frauddetection.simulator.controllers;

import cn.luixtech.dae.common.command.Command;
import cn.luixtech.dae.common.rule.RuleCommand;
import cn.luixtech.frauddetection.simulator.domain.Detector;
import cn.luixtech.frauddetection.simulator.kafka.producer.KafkaRuleProducer;
import cn.luixtech.frauddetection.simulator.repository.DetectorRepository;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@AllArgsConstructor
public class SyncRuleController {

    private final DetectorRepository detectorRepository;
    private final KafkaRuleProducer  kafkaRuleProducer;

    @GetMapping("/api/rules/sync-all")
    public void syncAllRules() {
        List<Detector> detectors = detectorRepository.findAll();
        for (Detector detector : detectors) {
            kafkaRuleProducer.addRule(detector.toRuleCommand());
        }
    }

    @GetMapping("/api/rules/delete-all")
    public void deleteAllRules() {
        RuleCommand ruleCommand = new RuleCommand();
        ruleCommand.setCommand(Command.DELETE_ALL);
        kafkaRuleProducer.addRule(ruleCommand);
    }
}
