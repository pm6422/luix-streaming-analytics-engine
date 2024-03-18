package com.luixtech.frauddetection.common.rule;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
public class AggregatingRule extends BaseRule {
    private String     aggregateFieldName;
    private Aggregator aggregator;
    private BigDecimal limit;
    private Integer    windowMinutes;
    private boolean    resetAfterMatch;

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on operator type.
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
                throw new RuntimeException("Unknown operator: " + operator);
        }
    }
}
