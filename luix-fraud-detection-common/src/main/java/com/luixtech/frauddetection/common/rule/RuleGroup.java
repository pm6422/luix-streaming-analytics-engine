package com.luixtech.frauddetection.common.rule;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Rules representation.
 */
@Data
@NoArgsConstructor
public class RuleGroup {
    /**
     * It will evaluate input data which have the same tenant.
     * It will evaluate all input data if the field is empty.
     */
    private String          tenant;
    /**
     * The ID of rule group
     */
    private String          id;
    /**
     * TODO: remove
     */
    private List<String>    groupingKeys;
    /**
     * The biggest time window of all the rules under sub group
     */
    private Integer         windowMinutes;
    private boolean         resetAfterMatch;
    private List<RuleGroup> children;
    private List<Rule>      rules;
    /**
     * Logical operator for multiple rule groups
     */
    private LogicalOperator logicalOperator = LogicalOperator.AND;
}
