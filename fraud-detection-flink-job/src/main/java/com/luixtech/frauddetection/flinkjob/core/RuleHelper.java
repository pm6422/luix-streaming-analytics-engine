package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.flinkjob.core.accumulator.AverageAccumulator;
import com.luixtech.frauddetection.flinkjob.core.accumulator.BigDecimalCounter;
import com.luixtech.frauddetection.flinkjob.core.accumulator.BigDecimalMaximum;
import com.luixtech.frauddetection.flinkjob.core.accumulator.BigDecimalMinimum;
import com.luixtech.frauddetection.flinkjob.domain.Rule;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

/* Collection of helper methods for Rules. */
public class RuleHelper {

    /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        switch (rule.getAggregatorFunctionType()) {
            case SUM:
                return new BigDecimalCounter();
            case AVG:
                return new AverageAccumulator();
            case MAX:
                return new BigDecimalMaximum();
            case MIN:
                return new BigDecimalMinimum();
            default:
                throw new RuntimeException(
                        "Unsupported aggregation function type: " + rule.getAggregatorFunctionType());
        }
    }
}
