package com.luixtech.frauddetection.common.input;

import com.luixtech.frauddetection.common.IngestionTimeAssignable;
import lombok.Data;

import java.util.Map;

@Data
public class InputRecord implements IngestionTimeAssignable {

    private String              recordId;
    private Map<String, Object> record;
    /**
     * Timestamp of ingestion into the flink input source, unit: ms
     */
    private Long                ingestionTime;

    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTime;
    }
}