package com.luixtech.frauddetection.common.rule;

import com.luixtech.frauddetection.common.command.Command;
import com.luixtech.frauddetection.common.IngestionTimeAssignable;
import lombok.Data;

/**
 * Control command with the rule group data
 */
@Data
public class RuleCommand implements IngestionTimeAssignable {
    /**
     * Command
     */
    private Command   command;
    /**
     * Rule group
     */
    private RuleGroup ruleGroup;
    /**
     * Created timestamp of the input, unit: ms
     */
    public  long      createdTime;
    /**
     * Timestamp of ingestion into the flink input source, unit: ms
     */
    private Long      ingestionTime;

    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTime;
    }
}
