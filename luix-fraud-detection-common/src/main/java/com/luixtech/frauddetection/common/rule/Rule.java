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
    private String             id;
    private List<String>       groupingKeyNames;
    private List<String>       unique;
    private String             aggregateFieldName;
    private AggregatorFunction aggregatorFunction;
    private LimitOperator      limitOperator;
    private BigDecimal         limit;
    private Integer            windowMinutes;
    private boolean            resetAfterMatch;

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperator) {
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
                throw new RuntimeException("Unknown limit operator type: " + limitOperator);
        }
    }

    public enum AggregatorFunction {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX
    }

    public enum LimitOperator {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER_EQUAL(">="),
        LESS_EQUAL("<="),
        GREATER(">"),
        LESS("<");

        final String operator;

        LimitOperator(String operator) {
            this.operator = operator;
        }

        public static LimitOperator fromString(String text) {
            for (LimitOperator b : LimitOperator.values()) {
                if (b.operator.equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }
}
