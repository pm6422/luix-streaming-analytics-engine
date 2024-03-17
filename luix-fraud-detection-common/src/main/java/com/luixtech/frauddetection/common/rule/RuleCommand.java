package com.luixtech.frauddetection.common.rule;

import com.luixtech.frauddetection.common.command.Command;
import com.luixtech.frauddetection.common.IngestionTimeAssignable;
import lombok.Data;

@Data
public class RuleCommand implements IngestionTimeAssignable {
    private Command command;
    private Rule rule;
    /**
     * Timestamp of ingestion into the flink input source, unit: milliseconds
     */
    private Long ingestionTime;

    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTime;
    }
}
