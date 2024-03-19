package com.luixtech.frauddetection.flinkjob.core.accumulator;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

/**
 * An accumulator that sums up {@code double} values.
 */
@PublicEvolving
public class BigDecimalSumAccumulator implements SimpleAccumulator<BigDecimal> {

    private static final long       serialVersionUID = 1L;
    private              BigDecimal localValue       = BigDecimal.ZERO;

    public BigDecimalSumAccumulator() {
    }

    public BigDecimalSumAccumulator(BigDecimal value) {
        this.localValue = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator
    // ------------------------------------------------------------------------

    @Override
    public void add(BigDecimal value) {
        localValue = localValue.add(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        return localValue;
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        localValue = localValue.add(other.getLocalValue());
    }

    @Override
    public void resetLocal() {
        this.localValue = BigDecimal.ZERO;
    }

    @Override
    public BigDecimalSumAccumulator clone() {
        BigDecimalSumAccumulator result = new BigDecimalSumAccumulator();
        result.localValue = localValue;
        return result;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "BigDecimalCounter " + this.localValue;
    }
}
