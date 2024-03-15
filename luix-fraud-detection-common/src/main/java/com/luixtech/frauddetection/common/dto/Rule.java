package com.luixtech.frauddetection.common.dto;

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
    private String                 id;
    private List<String>           groupingKeyNames;
    private List<String>           unique;
    private String                 aggregateFieldName;
    private AggregatorFunctionType aggregatorFunctionType;
    private LimitOperatorType      limitOperatorType;
    private BigDecimal             limit;
    private Integer                windowMinutes;
    private boolean                resetAfterMatch;

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperatorType) {
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
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
        }
    }

    public enum AggregatorFunctionType {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX
    }

    public enum LimitOperatorType {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER_EQUAL(">="),
        LESS_EQUAL("<="),
        GREATER(">"),
        LESS("<");

        final String operator;

        LimitOperatorType(String operator) {
            this.operator = operator;
        }

        public static LimitOperatorType fromString(String text) {
            for (LimitOperatorType b : LimitOperatorType.values()) {
                if (b.operator.equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }
}
