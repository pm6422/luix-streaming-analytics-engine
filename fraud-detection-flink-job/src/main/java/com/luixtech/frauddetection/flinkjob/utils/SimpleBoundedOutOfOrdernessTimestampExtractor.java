package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.common.dto.Transaction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends Transaction> extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrdernessMillis) {
        super(Time.of(outOfOrdernessMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
        return element.getEventTime();
    }
}