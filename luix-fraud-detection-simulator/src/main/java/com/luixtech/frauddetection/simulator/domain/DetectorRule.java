package com.luixtech.frauddetection.simulator.domain;

import com.luixtech.frauddetection.common.command.Control;
import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.dto.RuleCommand;
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
    private String                      aggregateFieldName;
    private Rule.AggregatorFunctionType aggregatorFunctionType;
    private Rule.LimitOperatorType      limitOperatorType;
    private BigDecimal                  limit;
    private Integer                     windowMinutes;
    private boolean                     resetAfterMatch;
    private Boolean                     enabled;

    public RuleCommand toRuleCommand() {
        RuleCommand ruleCommand = new RuleCommand();
        if (Boolean.TRUE.equals(this.enabled)) {
            ruleCommand.setControl(Control.ADD);
        } else {
            ruleCommand.setControl(Control.DELETE);
        }
        Rule rule = new Rule();
        BeanUtils.copyProperties(this, rule);
        ruleCommand.setRule(rule);
        return ruleCommand;
    }
}
