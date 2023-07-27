package com.luixtech.frauddetection.flinkjob.dynamicrules;

public interface TimestampAssignable<T> {
    void assignIngestionTimestamp(T timestamp);
}
