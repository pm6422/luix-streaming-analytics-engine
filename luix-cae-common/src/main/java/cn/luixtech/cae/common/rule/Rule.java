package cn.luixtech.cae.common.rule;

import cn.luixtech.cae.common.input.Input;
import cn.luixtech.cae.common.rule.aggregating.AggregatingRule;
import cn.luixtech.cae.common.rule.matching.MatchingRule;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

/**
 * Rules representation.
 */
@Data
@NoArgsConstructor
public class Rule {
    private static final String MAPPING_INPUT_RECORD_MSG   = "msg";
    private static final String MAPPING_INPUT_RECORD_EVENT = "event";

    /**
     * Arithmetic operator
     */
    private ArithmeticOperator arithmeticOperator;
    /**
     * The time window of the rule
     */
    private Integer            windowMinutes;
    /**
     * Matching rule fields
     */
    private MatchingRule       matchingRule;
    /**
     * Aggregating rule fields
     */
    private AggregatingRule    aggregatingRule;
    /**
     * TODO：
     * The actual data storing in field 'record' of {@link Input} class
     */
    private String             mappingInputRecord;
    /**
     * The reference key for the rule expression storing in 'record' map of {@link Input} class
     * e.g. rule expression: model == X9, referenceRecordKey = state
     * We can get model by using input.record.get("state").get("model")
     */
    private String             referenceRecordKey;
    /**
     * The target key for the rule expression storing in 'record' map of {@link Input} class
     * e.g. rule expression: speed == engineSpeed, targetRecordKey = msg
     * We can get engineSpeed by using input.record.get("msg").get("engineSpeed")
     */
    private String             targetRecordKey;
    /**
     * It represents logical relationship for this rule with the next rule
     */
    private LogicalOperator    logicalOperator = LogicalOperator.AND;

    public RuleType determineType() {
        if (aggregatingRule != null && matchingRule == null) {
            return RuleType.AGGREGATING;
        } else if (matchingRule != null && aggregatingRule == null) {
            return RuleType.MATCHING;
        }
        throw new RuntimeException("Unsupported rule type");
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMappingRecord(Input input) {
        if (StringUtils.isEmpty(mappingInputRecord)) {
            return input.getRecord();
        }
        if (!input.getRecord().containsKey(mappingInputRecord)) {
            // return empty if not exist
            return Collections.emptyMap();
        }
        return (Map<String, Object>) input.getRecord().get(mappingInputRecord);
    }
}
