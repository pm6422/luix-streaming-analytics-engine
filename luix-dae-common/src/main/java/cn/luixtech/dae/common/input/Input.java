package cn.luixtech.dae.common.input;

import cn.luixtech.dae.common.IngestionTimeAssignable;
import cn.luixtech.dae.common.rule.Rule;
import cn.luixtech.dae.common.rule.RuleGroup;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
     * "ID of the entity to which the record belongs
     */
    private String              entityId;
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
    private long                ingestionTime;
    /**
     * Grouping field name and value pair, used to sharding the input records by flink,
     * the field is used in combination with field 'groupingKeys' of {@link RuleGroup}
     */
    private Map<String, Object> groupingValues;

    @Override
    public void setIngestionTime(long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public long getIngestionTime() {
        return this.ingestionTime;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getRecRefMap(Rule rule) {
        if (MapUtils.isEmpty(record)) {
            return new HashMap<>();
        }
        if (StringUtils.isEmpty(rule.getReferenceRecordKey())) {
            return record;
        }
        return ((Map<String, Object>) record.get(rule.getReferenceRecordKey()));
    }

    @SuppressWarnings("unchecked")
    public String getRecRefFieldValue(Rule rule, String fieldName) {
        if (MapUtils.isEmpty(record)) {
            return StringUtils.EMPTY;
        }
        if (StringUtils.isEmpty(rule.getReferenceRecordKey())) {
            return Objects.toString(record.get(fieldName), StringUtils.EMPTY);
        }
        Object fieldValue = ((Map<String, Object>) record.get(rule.getReferenceRecordKey())).get(fieldName);
        return Objects.toString(fieldValue, StringUtils.EMPTY);
    }

    @SuppressWarnings("unchecked")
    public String getRecTargetFieldValue(Rule rule, String fieldName) {
        if (MapUtils.isEmpty(record)) {
            return StringUtils.EMPTY;
        }
        if (StringUtils.isEmpty(rule.getTargetRecordKey())) {
            return Objects.toString(record.get(fieldName), StringUtils.EMPTY);
        }
        Object fieldValue = ((Map<String, Object>) record.get(rule.getTargetRecordKey())).get(fieldName);
        return Objects.toString(fieldValue, StringUtils.EMPTY);
    }
}