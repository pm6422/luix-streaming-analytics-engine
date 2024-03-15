package com.luixtech.frauddetection.common.dto;

import com.luixtech.frauddetection.common.command.Control;
import lombok.Data;

@Data
public class RuleCommand implements TimestampAssignable {
    private Control control;
    private Rule    rule;
    /**
     * Timestamp of ingestion into the flink input source, unit: milliseconds
     */
    private Long    ingestionTimestamp;

    @Override
    public void setIngestionTimestamp(Long timestamp) {
        this.ingestionTimestamp = timestamp;
    }

    @Override
    public Long getIngestionTimestamp() {
        return this.ingestionTimestamp;
    }
}
