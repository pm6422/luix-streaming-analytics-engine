package com.luixtech.frauddetection.simulator.config.dbmigrations;

import com.luixtech.frauddetection.common.rule.aggregating.AggregatingRule;
import com.luixtech.frauddetection.common.rule.aggregating.Aggregator;
import com.luixtech.frauddetection.common.rule.Operator;
import com.luixtech.frauddetection.simulator.domain.DetectorRule;
import com.luixtech.frauddetection.simulator.repository.DetectorRuleRepository;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

@Component
@AllArgsConstructor
public class InitialSetupMigration implements ApplicationRunner {

    private final DetectorRuleRepository detectorRuleRepository;

    public void run(ApplicationArguments args) {
        AggregatingRule aggregatingRule1 = new AggregatingRule();
        aggregatingRule1.setAggregateFieldName("paymentAmount");
        aggregatingRule1.setAggregator(Aggregator.SUM);
        aggregatingRule1.setExpectedLimitValue(new BigDecimal("20000000"));

        DetectorRule detectorRule1 = new DetectorRule();
        detectorRule1.setId("1");
        detectorRule1.setGroupingKeys(Arrays.asList("payeeId", "beneficiaryId"));
        detectorRule1.setOperator(Operator.GREATER);
        detectorRule1.setWindowMinutes(43200);
        detectorRule1.setEnabled(true);
        detectorRule1.setAggregatingRule(aggregatingRule1);

        AggregatingRule aggregatingRule2 = new AggregatingRule();
        aggregatingRule2.setAggregator(Aggregator.COUNT);
        aggregatingRule2.setExpectedLimitValue(new BigDecimal("300"));

        DetectorRule detectorRule2 = new DetectorRule();
        detectorRule2.setId("2");
        detectorRule2.setGroupingKeys(List.of("paymentType"));
        detectorRule2.setOperator(Operator.LESS);
        detectorRule2.setWindowMinutes(1440);
        detectorRule2.setEnabled(false);
        detectorRule2.setAggregatingRule(aggregatingRule2);

        AggregatingRule aggregatingRule3 = new AggregatingRule();
        aggregatingRule3.setAggregateFieldName("paymentAmount");
        aggregatingRule3.setAggregator(Aggregator.SUM);
        aggregatingRule3.setExpectedLimitValue(new BigDecimal("10000000"));

        DetectorRule detectorRule3 = new DetectorRule();
        detectorRule3.setId("3");
        detectorRule3.setGroupingKeys(List.of("beneficiaryId"));
        detectorRule3.setOperator(Operator.GREATER_EQUAL);
        detectorRule3.setWindowMinutes(1440);
        detectorRule3.setEnabled(true);
        detectorRule3.setAggregatingRule(aggregatingRule3);

        AggregatingRule aggregatingRule4 = new AggregatingRule();
        aggregatingRule4.setAggregator(Aggregator.COUNT);
        aggregatingRule4.setExpectedLimitValue(new BigDecimal("100"));

        DetectorRule detectorRule4 = new DetectorRule();
        detectorRule4.setId("4");
        detectorRule4.setGroupingKeys(List.of("paymentType"));
        detectorRule4.setOperator(Operator.GREATER_EQUAL);
        detectorRule4.setWindowMinutes(1440);
        detectorRule4.setResetAfterMatch(true);
        detectorRule4.setEnabled(true);
        detectorRule4.setAggregatingRule(aggregatingRule4);

        detectorRuleRepository.save(detectorRule1);
        detectorRuleRepository.save(detectorRule2);
        detectorRuleRepository.save(detectorRule3);
        detectorRuleRepository.save(detectorRule4);
    }
}
