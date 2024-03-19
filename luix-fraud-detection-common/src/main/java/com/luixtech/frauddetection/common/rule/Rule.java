package com.luixtech.frauddetection.common.rule;

import com.luixtech.frauddetection.common.input.Input;
import com.luixtech.frauddetection.common.rule.aggregating.AggregatingRule;
import com.luixtech.frauddetection.common.rule.matching.MatchingRule;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Rules representation.
 */
@Data
@NoArgsConstructor
public class Rule {
    private static final String MAPPING_INPUT_RECORD_MSG   = "msg";
    private static final String MAPPING_INPUT_RECORD_EVENT = "event";

    private Operator        operator;
    private Integer         windowMinutes;
    /**
     * Matching rule fields
     */
    private MatchingRule    matchingRule;
    /**
     * Aggregating rule fields
     */
    private AggregatingRule aggregatingRule;
    /**
     * The actual data storing in field 'record' of {@link Input} class
     */
    private String          mappingInputRecord;
    /**
     * Logical operator for multiple rules
     */
    private LogicalOperator logicalOperator;

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
