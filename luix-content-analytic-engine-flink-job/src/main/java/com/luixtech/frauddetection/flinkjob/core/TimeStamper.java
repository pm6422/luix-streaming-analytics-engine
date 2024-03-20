package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.IngestionTimeAssignable;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class TimeStamper<T extends IngestionTimeAssignable> extends RichFlatMapFunction<T, T> {

    @Override
    public void flatMap(T value, Collector<T> out) {
        value.setIngestionTime(System.currentTimeMillis());
        out.collect(value);
    }
}
