package cn.luixtech.dae.common.rule.aggregating;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class AggregatingRule {
    private String     aggregateFieldName;
    private String     expectedAggregateFieldValue;
    private Aggregator aggregator;
    private BigDecimal expectedLimitValue;
    private BigDecimal actualAggregatedValue;
}
