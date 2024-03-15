package com.luixtech.frauddetection.simulator.config.dbmigrations;

import com.luixtech.frauddetection.common.pojo.Rule;
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
        DetectorRule detectorRule1 = new DetectorRule();
        detectorRule1.setId("1");
        detectorRule1.setAggregateFieldName("paymentAmount");
        detectorRule1.setAggregatorFunctionType(Rule.AggregatorFunctionType.SUM);
        detectorRule1.setGroupingKeyNames(Arrays.asList("payeeId", "beneficiaryId"));
        detectorRule1.setLimit(new BigDecimal("20000000"));
        detectorRule1.setLimitOperatorType(Rule.LimitOperatorType.GREATER);
        detectorRule1.setWindowMinutes(43200);
        detectorRule1.setEnabled(true);

        DetectorRule detectorRule2 = new DetectorRule();
        detectorRule2.setId("2");
        detectorRule2.setAggregatorFunctionType(Rule.AggregatorFunctionType.COUNT);
        detectorRule2.setGroupingKeyNames(List.of("paymentType"));
        detectorRule2.setLimit(new BigDecimal("300"));
        detectorRule2.setLimitOperatorType(Rule.LimitOperatorType.LESS);
        detectorRule2.setWindowMinutes(1440);
        detectorRule2.setEnabled(false);

        DetectorRule detectorRule3 = new DetectorRule();
        detectorRule3.setId("3");
        detectorRule3.setAggregateFieldName("paymentAmount");
        detectorRule3.setAggregatorFunctionType(Rule.AggregatorFunctionType.SUM);
        detectorRule3.setGroupingKeyNames(List.of("beneficiaryId"));
        detectorRule3.setLimit(new BigDecimal("10000000"));
        detectorRule3.setLimitOperatorType(Rule.LimitOperatorType.GREATER_EQUAL);
        detectorRule3.setWindowMinutes(1440);
        detectorRule3.setEnabled(true);

        DetectorRule detectorRule4 = new DetectorRule();
        detectorRule4.setId("4");
        detectorRule4.setAggregatorFunctionType(Rule.AggregatorFunctionType.COUNT);
        detectorRule4.setGroupingKeyNames(List.of("paymentType"));
        detectorRule4.setLimit(new BigDecimal("100"));
        detectorRule4.setLimitOperatorType(Rule.LimitOperatorType.GREATER_EQUAL);
        detectorRule4.setWindowMinutes(1440);
        detectorRule4.setResetAfterMatch(true);
        detectorRule4.setEnabled(true);

        detectorRuleRepository.save(detectorRule1);
        detectorRuleRepository.save(detectorRule2);
        detectorRuleRepository.save(detectorRule3);
        detectorRuleRepository.save(detectorRule4);
    }
}
