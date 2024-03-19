package com.luixtech.frauddetection.common.rule;

import com.luixtech.frauddetection.common.rule.aggregating.AggregatingRule;
import com.luixtech.frauddetection.common.rule.matching.MatchingRule;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rules representation.
 */
@Data
@NoArgsConstructor
public class Rule {
    private String          id;
    private List<String>    groupingKeys;
    private Operator        operator;
    private Integer         windowMinutes;
    private boolean         resetAfterMatch;
    /**
     * Matching rule fields
     */
    private MatchingRule    matchingRule;
    /**
     * Aggregating rule fields
     */
    private AggregatingRule aggregatingRule;


    public RuleType determineType() {
        if (aggregatingRule != null) {
            return RuleType.AGGREGATING;
        } else if (matchingRule != null) {
            return RuleType.MATCHING;
        }
        throw new RuntimeException("Unsupported rule type");
    }
}
