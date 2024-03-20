package cn.luixtech.frauddetection.simulator.config.dbmigrations;

import cn.luixtech.cae.common.rule.Rule;
import cn.luixtech.cae.common.rule.RuleGroup;
import cn.luixtech.cae.common.rule.aggregating.AggregatingRule;
import cn.luixtech.cae.common.rule.aggregating.Aggregator;
import cn.luixtech.cae.common.rule.ArithmeticOperator;
import cn.luixtech.frauddetection.simulator.domain.Detector;
import cn.luixtech.frauddetection.simulator.repository.DetectorRepository;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Arrays;

@Component
@AllArgsConstructor
public class InitialSetupMigration implements ApplicationRunner {

    private final DetectorRepository detectorRepository;

    public void run(ApplicationArguments args) {
        // rule 1
        AggregatingRule aggregatingRule1 = new AggregatingRule();
        aggregatingRule1.setAggregateFieldName("paymentAmount");
        aggregatingRule1.setAggregator(Aggregator.SUM);
        aggregatingRule1.setExpectedLimitValue(new BigDecimal("20000000"));

        Rule rule1 = new Rule();
        rule1.setArithmeticOperator(ArithmeticOperator.GREATER);
        rule1.setAggregatingRule(aggregatingRule1);
        rule1.setWindowMinutes(43200);

        RuleGroup ruleGroup1 = new RuleGroup();
        ruleGroup1.setId("1");
        ruleGroup1.setTenant("tesla");
        ruleGroup1.setWindowMinutes(rule1.getWindowMinutes());
        ruleGroup1.setRules(Arrays.asList(rule1));

        Detector detector1 = new Detector();
        detector1.setId(ruleGroup1.getId());
        detector1.setTenant(ruleGroup1.getTenant());
        detector1.setEnabled(true);
        detector1.setRuleGroup(ruleGroup1);


        // rule 3
        AggregatingRule aggregatingRule3 = new AggregatingRule();
        aggregatingRule3.setAggregateFieldName("paymentAmount");
        aggregatingRule3.setAggregator(Aggregator.SUM);
        aggregatingRule3.setExpectedLimitValue(new BigDecimal("10000000"));

        Rule rule3 = new Rule();
        rule3.setArithmeticOperator(ArithmeticOperator.GREATER_EQUAL);
        rule3.setAggregatingRule(aggregatingRule3);
        rule3.setWindowMinutes(1440);

        RuleGroup ruleGroup3 = new RuleGroup();
        ruleGroup3.setId("3");
        ruleGroup3.setTenant("honda");
        ruleGroup3.setWindowMinutes(rule3.getWindowMinutes());
        ruleGroup3.setRules(Arrays.asList(rule3));

        Detector detector3 = new Detector();
        detector3.setId(ruleGroup3.getId());
        detector3.setTenant(ruleGroup3.getTenant());
        detector3.setEnabled(true);
        detector3.setRuleGroup(ruleGroup3);

        // rule 4
        AggregatingRule aggregatingRule4 = new AggregatingRule();
        aggregatingRule4.setAggregator(Aggregator.COUNT);
        aggregatingRule4.setExpectedLimitValue(new BigDecimal("100"));

        Rule rule4 = new Rule();
        rule4.setArithmeticOperator(ArithmeticOperator.GREATER_EQUAL);
        rule4.setAggregatingRule(aggregatingRule3);
        rule4.setWindowMinutes(1440);

        RuleGroup ruleGroup4 = new RuleGroup();
        ruleGroup4.setId("4");
        ruleGroup4.setTenant("tesla");
        ruleGroup4.setWindowMinutes(rule4.getWindowMinutes());
        ruleGroup4.setRules(Arrays.asList(rule4));
        ruleGroup4.setResetAfterMatch(true);

        Detector detector4 = new Detector();
        detector4.setId("4");
        detector4.setTenant(ruleGroup4.getTenant());
        detector4.setEnabled(true);
        detector4.setRuleGroup(ruleGroup4);

        detectorRepository.save(detector1);
        detectorRepository.save(detector3);
        detectorRepository.save(detector4);
    }
}
