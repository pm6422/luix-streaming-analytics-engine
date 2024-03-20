package cn.luixtech.cae.common.input;

import cn.luixtech.cae.common.IngestionTimeAssignable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import cn.luixtech.cae.common.rule.RuleGroup;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Input implements IngestionTimeAssignable {
    /**
     * It will be evaluated by rule groups which have the same tenant.
     */
    private String              tenant;
    /**
     * ID of the record
     */
    private String              recordId;
    /**
     * Input record data which can be a nested structure
     */
    private Map<String, Object> record;
    /**
     * Created timestamp of the input record, unit: ms
     */
    public  long                createdTime;
    /**
     * Timestamp of ingestion into the flink input source, unit: ms
     */
    private Long                ingestionTime;
    /**
     * Grouping field name and value pair, used to sharding the input records by flink,
     * the field is used in combination with field 'groupingKeys' of {@link RuleGroup}
     */
    private Map<String, Object> groupingValues;

    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTime;
    }
}