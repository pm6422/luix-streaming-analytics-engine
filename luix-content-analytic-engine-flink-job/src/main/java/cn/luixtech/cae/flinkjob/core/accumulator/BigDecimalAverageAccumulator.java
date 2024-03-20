package cn.luixtech.cae.flinkjob.core.accumulator;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

/**
 * An accumulator that computes the average value. Input can be {@code long}, {@code integer}, or
 * {@code double} and the result is {@code double}.
 */
@Public
public class BigDecimalAverageAccumulator implements SimpleAccumulator<BigDecimal> {

    private static final long       serialVersionUID = 1L;
    private              long       count;
    private              BigDecimal sum;

    @Override
    public void add(BigDecimal value) {
        this.count++;
        this.sum = sum.add(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        if (this.count == 0) {
            return BigDecimal.ZERO;
        }
        return this.sum.divide(new BigDecimal(count));
    }

    @Override
    public void resetLocal() {
        this.count = 0;
        this.sum = BigDecimal.ZERO;
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        if (other instanceof BigDecimalAverageAccumulator) {
            BigDecimalAverageAccumulator avg = (BigDecimalAverageAccumulator) other;
            this.count += avg.count;
            this.sum = sum.add(avg.sum);
        } else {
            throw new IllegalArgumentException("The merged accumulator must be AverageAccumulator.");
        }
    }

    @Override
    public BigDecimalAverageAccumulator clone() {
        BigDecimalAverageAccumulator average = new BigDecimalAverageAccumulator();
        average.count = this.count;
        average.sum = this.sum;
        return average;
    }

    @Override
    public String toString() {
        return "AverageAccumulator " + this.getLocalValue() + " for " + this.count + " elements";
    }
}
