package cn.luixtech.dae.common.rule;

import cn.luixtech.dae.common.command.Command;
import cn.luixtech.dae.common.IngestionTimeAssignable;
import lombok.Data;

/**
 * Control command with the rule group data
 */
@Data
public class RuleCommand implements IngestionTimeAssignable {
    /**
     * Dynamically control the rules without needing application restart
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
    private long      ingestionTime;

    @Override
    public void setIngestionTime(long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public long getIngestionTime() {
        return this.ingestionTime;
    }
}
