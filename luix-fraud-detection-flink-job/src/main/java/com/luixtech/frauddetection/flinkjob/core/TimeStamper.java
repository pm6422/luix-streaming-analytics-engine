package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.dto.TimestampAssignable;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class TimeStamper<T extends TimestampAssignable> extends RichFlatMapFunction<T, T> {

    @Override
    public void flatMap(T value, Collector<T> out) {
        value.setIngestionTimestamp(System.currentTimeMillis());
        out.collect(value);
    }
}
