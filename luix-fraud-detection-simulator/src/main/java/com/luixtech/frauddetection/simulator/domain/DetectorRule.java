package com.luixtech.frauddetection.simulator.domain;

import com.luixtech.frauddetection.common.command.Command;
import com.luixtech.frauddetection.common.rule.Rule;
import com.luixtech.frauddetection.common.rule.RuleCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.util.List;

/**
 * Spring Data MongoDB collection for the DetectorRule entity.
 */
@Document
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DetectorRule {
    @Id
    private String                      id;
    private List<String>                groupingKeyNames;
    private List<String>                unique;
    private String                  aggregateFieldName;
    private Rule.AggregatorFunction aggregatorFunction;
    private Rule.LimitOperator      limitOperator;
    private BigDecimal                  limit;
    private Integer                     windowMinutes;
    private boolean                     resetAfterMatch;
    private Boolean                     enabled;

    public RuleCommand toRuleCommand() {
        RuleCommand ruleCommand = new RuleCommand();
        if (Boolean.TRUE.equals(this.enabled)) {
            ruleCommand.setCommand(Command.ADD);
        } else {
            ruleCommand.setCommand(Command.DELETE);
        }
        Rule rule = new Rule();
        BeanUtils.copyProperties(this, rule);
        ruleCommand.setRule(rule);
        return ruleCommand;
    }
}
