package com.luixtech.frauddetection.common.rule;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Rules representation.
 */
@Data
@NoArgsConstructor
public class Rule {
    private String       id;
    private List<String> groupingKeys;
    private Operator     operator;

    /**
     * Matching rule fields
     */
    private String fieldName;
    private String expectedValue;
    private String expectedFieldName;

    /**
     * Aggregating rule fields
     */
    private String     aggregateFieldName;
    private Aggregator aggregator;
    private BigDecimal limit;
    private Integer    windowMinutes;
    private boolean    resetAfterMatch;

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean evaluate(Map<String, Object> matchingRecord, BigDecimal comparisonValue) {
        if (RuleType.AGGREGATING == determineType()) {
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
        } else {
            if (StringUtils.isNotEmpty(expectedValue)) {
                return expectedValue.equals(String.valueOf(matchingRecord.get(fieldName)));
            } else {
                return matchingRecord.get(expectedFieldName).equals(matchingRecord.get(fieldName));
            }
        }
    }

    private RuleType determineType() {
        if (aggregator != null && limit != null && windowMinutes != null) {
            return RuleType.AGGREGATING;
        } else if (StringUtils.isNotEmpty(fieldName)
                && (StringUtils.isNotEmpty(expectedValue) || StringUtils.isNotEmpty(expectedFieldName))) {
            return RuleType.MATCHING;
        }
        throw new RuntimeException("Unknown rule type");
    }
}
