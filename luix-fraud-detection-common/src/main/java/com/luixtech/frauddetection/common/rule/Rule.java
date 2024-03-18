package com.luixtech.frauddetection.common.rule;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rules representation.
 */
@Data
@NoArgsConstructor
public class Rule {
    private String       id;
    private List<String> groupingKeys;
    private String       aggregateFieldName;
    private Aggregator   aggregator;
    private Operator     operator;
    private BigDecimal   limit;
    private Integer      windowMinutes;
    private boolean      resetAfterMatch;

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean evaluate(BigDecimal comparisonValue) {
        switch (operator) {
            case EQUAL:
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                return comparisonValue.compareTo(limit) > 0;
            case LESS:
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(limit) <= 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("Unknown operator type: " + operator);
        }
    }
}
