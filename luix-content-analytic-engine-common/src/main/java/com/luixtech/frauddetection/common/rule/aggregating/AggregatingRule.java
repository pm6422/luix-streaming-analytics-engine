package com.luixtech.frauddetection.common.rule.aggregating;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class AggregatingRule {
    private String     aggregateFieldName;
    private Aggregator aggregator;
    private BigDecimal expectedLimitValue;
    private BigDecimal actualAggregatedValue;
}
