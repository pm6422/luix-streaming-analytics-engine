package com.luixtech.frauddetection.flinkjob.core.function;

import com.luixtech.frauddetection.common.dto.TimestampAssignable;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class TimeStamper<T extends TimestampAssignable<Long>> extends RichFlatMapFunction<T, T> {

    @Override
    public void flatMap(T value, Collector<T> out) {
        value.assignIngestionTimestamp(System.currentTimeMillis());
        out.collect(value);
    }
}
