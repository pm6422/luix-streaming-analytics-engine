package cn.luixtech.dae.common.rule;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A rule group consists of a set of rules or child rule groups.
 */
@Data
@NoArgsConstructor
public class RuleGroup {
    /**
     * The ID of rule group
     */
    private String            id;
    /**
     * It will evaluate input data which have the same tenant or evaluate all input data if this field is empty
     */
    private String            tenant;
    /**
     * The widest time window of all the rules which is used to evict the aged inputs
     */
    private Integer           windowMinutes;
    /**
     * The rule group will not issue same output for same input entity during the silent period to prevent excessive output from interfering. 0 represents no silent period
     */
    private int               silentMinutes;
    /**
     * Last matching time that the rule group matches the inputs
     */
    private Map<String, Long> lastMatchingTime = new HashMap();
    /**
     * Clear local existing inputs cache after rule matched
     */
    private boolean           resetAfterMatch;
    /**
     * It represents logical relationship for this rule group with the next rule group
     */
    private LogicalOperator   logicalOperator  = LogicalOperator.AND;
    /**
     * Child rule groups, and it is necessary to ensure physical order
     */
    private List<RuleGroup>   children;
    /**
     * Rules under the group, and it is necessary to ensure physical order
     */
    private List<Rule>        rules;

    public void addLastMatchingTime(String entityId, Long matchingTime) {
        lastMatchingTime.put(entityId, matchingTime);
    }
}
