package com.luixtech.frauddetection.common.rule;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

@Data
@NoArgsConstructor
public class MatchingRule extends BaseRule {
    private String fieldName;
    private String expectedValue;
    private String expectedFieldName;

    @Override
    protected RuleType getRuleType() {
        return RuleType.MATCHING;
    }

    public boolean evaluate(Map<String, Object> inputRecord) {
        if (StringUtils.isNotEmpty(expectedValue)) {
            return expectedValue.equals(String.valueOf(inputRecord.get(fieldName)));
        } else {
            return inputRecord.get(expectedFieldName).equals(inputRecord.get(fieldName));
        }
    }
}
