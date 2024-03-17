package com.luixtech.frauddetection.common;

public interface IngestionTimeAssignable {
    void setIngestionTime(Long timestamp);

    Long getIngestionTime();
}
