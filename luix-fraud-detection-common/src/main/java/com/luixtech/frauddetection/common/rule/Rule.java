package com.luixtech.frauddetection.common.rule;

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
    private BigDecimal actualAggregatedValue;


    public RuleType determineType() {
        if (aggregator != null && limit != null && windowMinutes != null) {
            return RuleType.AGGREGATING;
        } else if (StringUtils.isNotEmpty(fieldName)
                && (StringUtils.isNotEmpty(expectedValue) || StringUtils.isNotEmpty(expectedFieldName))) {
            return RuleType.MATCHING;
        }
        throw new RuntimeException("Unknown rule type");
    }
}
