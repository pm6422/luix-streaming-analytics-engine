package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.common.dto.TimestampAssignable;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends TimestampAssignable> extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int maxOutOfOrderness) {
        super(Time.of(maxOutOfOrderness, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
        return element.getIngestionTimestamp();
    }
}