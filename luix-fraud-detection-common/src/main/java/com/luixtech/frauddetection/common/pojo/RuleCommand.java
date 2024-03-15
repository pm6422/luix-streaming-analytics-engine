package com.luixtech.frauddetection.common.pojo;

import com.luixtech.frauddetection.common.command.Control;
import com.luixtech.frauddetection.common.pojo.base.IngestionTimeAssignable;
import lombok.Data;

@Data
public class RuleCommand implements IngestionTimeAssignable {
    private Control control;
    private Rule    rule;
    /**
     * Timestamp of ingestion into the flink input source, unit: milliseconds
     */
    private Long    ingestionTimestamp;

    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTimestamp = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTimestamp;
    }
}
