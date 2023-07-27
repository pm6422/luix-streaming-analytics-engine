package com.luixtech.frauddetection.common.dto;

public interface TimestampAssignable<T> {
    void assignIngestionTimestamp(T timestamp);
}
