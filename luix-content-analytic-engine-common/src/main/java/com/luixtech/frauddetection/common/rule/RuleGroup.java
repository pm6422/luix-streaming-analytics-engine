package com.luixtech.frauddetection.common.rule;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * A rule group consists of a set of rules or child rule groups.
 */
@Data
@NoArgsConstructor
public class RuleGroup {
    /**
     * The ID of rule group
     */
    private String          id;
    /**
     * It will evaluate input data which have the same tenant.
     * It will evaluate all input data if the field is empty.
     */
    private String          tenant;
    /**
     * The biggest time window of all the rules
     */
    private Integer         windowMinutes;
    /**
     * Clear local existing inputs cache after rule matched
     */
    private boolean         resetAfterMatch;
    /**
     * It represents logical relationship for this rule group with the next rule group
     */
    private LogicalOperator logicalOperator = LogicalOperator.AND;
    /**
     * child rule groups, and it is necessary to ensure logical order
     */
    private List<RuleGroup> children;
    /**
     * Rules under the group, and it is necessary to ensure logical order
     */
    private List<Rule>      rules;
}
