package com.luixtech.frauddetection.common.rule;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class RuleEvaluationResult {
    private boolean matched;

    private BigDecimal aggregateResult;
}
