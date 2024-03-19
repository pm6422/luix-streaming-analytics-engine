package com.luixtech.frauddetection.simulator.domain;

import com.luixtech.frauddetection.common.command.Command;
import com.luixtech.frauddetection.common.rule.RuleGroup;
import com.luixtech.frauddetection.common.rule.aggregating.AggregatingRule;
import com.luixtech.frauddetection.common.rule.Operator;
import com.luixtech.frauddetection.common.rule.RuleCommand;
import com.luixtech.frauddetection.common.rule.matching.MatchingRule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

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
    private String          id;
    private List<String>    groupingKeys;
    private Operator        operator;
    private Integer         windowMinutes;
    private boolean         resetAfterMatch;
    private Boolean         enabled;
    /**
     * Matching rule fields
     */
    private MatchingRule    matchingRule;
    /**
     * Aggregating rule fields
     */
    private AggregatingRule aggregatingRule;

    public RuleCommand toRuleCommand() {
        RuleCommand ruleCommand = new RuleCommand();
        if (Boolean.TRUE.equals(this.enabled)) {
            ruleCommand.setCommand(Command.ADD);
        } else {
            ruleCommand.setCommand(Command.DELETE);
        }
        RuleGroup ruleGroup = new RuleGroup();
        BeanUtils.copyProperties(this, ruleGroup);
        ruleCommand.setCreatedTime(System.currentTimeMillis());
        ruleCommand.setRuleGroup(ruleGroup);
        return ruleCommand;
    }
}
