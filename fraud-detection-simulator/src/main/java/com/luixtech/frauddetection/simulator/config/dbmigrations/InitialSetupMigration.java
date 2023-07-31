package com.luixtech.frauddetection.simulator.config.dbmigrations;

import com.luixtech.frauddetection.simulator.domain.RulePayload;
import com.luixtech.frauddetection.simulator.repository.RuleRepository;
import com.luixtech.frauddetection.simulator.kafka.producer.KafkaRuleProducer;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@AllArgsConstructor
public class InitialSetupMigration implements ApplicationRunner {

    private final RuleRepository    ruleRepository;
    private final KafkaRuleProducer kafkaRuleProducer;

    public void run(ApplicationArguments args) {
        String payload1 =
                "{\"ruleId\":\"1\","
                        + "\"aggregateFieldName\":\"paymentAmount\","
                        + "\"aggregatorFunctionType\":\"SUM\","
                        + "\"groupingKeyNames\":[\"payeeId\", \"beneficiaryId\"],"
                        + "\"limit\":\"20000000\","
                        + "\"limitOperatorType\":\"GREATER\","
                        + "\"ruleState\":\"ACTIVE\","
                        + "\"windowMinutes\":\"43200\"}";

        RulePayload rulePayload1 = new RulePayload(payload1);

        String payload2 =
                "{\"ruleId\":\"2\","
                        + "\"aggregateFieldName\":\"\","
                        + "\"aggregatorFunctionType\":\"COUNT\","
                        + "\"groupingKeyNames\":[\"paymentType\"],"
                        + "\"limit\":\"300\","
                        + "\"limitOperatorType\":\"LESS\","
                        + "\"ruleState\":\"PAUSE\","
                        + "\"windowMinutes\":\"1440\"}";

        RulePayload rulePayload2 = new RulePayload(payload2);

        String payload3 =
                "{\"ruleId\":\"3\","
                        + "\"aggregateFieldName\":\"paymentAmount\","
                        + "\"aggregatorFunctionType\":\"SUM\","
                        + "\"groupingKeyNames\":[\"beneficiaryId\"],"
                        + "\"limit\":\"10000000\","
                        + "\"limitOperatorType\":\"GREATER_EQUAL\","
                        + "\"ruleState\":\"ACTIVE\","
                        + "\"windowMinutes\":\"1440\"}";

        RulePayload rulePayload3 = new RulePayload(payload3);

        String payload4 =
                "{\"ruleId\":\"4\","
                        + "\"aggregateFieldName\":\"\","
                        + "\"aggregatorFunctionType\":\"COUNT_WITH_RESET\","
                        + "\"groupingKeyNames\":[\"paymentType\"],"
                        + "\"limit\":\"100\","
                        + "\"limitOperatorType\":\"GREATER_EQUAL\","
                        + "\"ruleState\":\"ACTIVE\","
                        + "\"windowMinutes\":\"1440\"}";

        RulePayload rulePayload4 = new RulePayload(payload4);

        ruleRepository.save(rulePayload1);
        ruleRepository.save(rulePayload2);
        ruleRepository.save(rulePayload3);
        ruleRepository.save(rulePayload4);

        List<RulePayload> rulePayloads = ruleRepository.findAll();
        rulePayloads.stream().map(RulePayload::toRule).forEach(kafkaRuleProducer::addRule);
    }
}
