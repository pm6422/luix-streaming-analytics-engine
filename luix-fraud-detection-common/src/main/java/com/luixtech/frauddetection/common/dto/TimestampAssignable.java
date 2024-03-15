package com.luixtech.frauddetection.common.dto;

public interface TimestampAssignable {
    void setIngestionTimestamp(Long timestamp);

    Long getIngestionTimestamp();
}
