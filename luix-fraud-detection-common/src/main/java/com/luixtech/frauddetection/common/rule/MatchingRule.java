package com.luixtech.frauddetection.common.rule;

import com.luixtech.frauddetection.common.rule.base.BaseRule;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

@Deprecated
@Data
@NoArgsConstructor
public class MatchingRule extends BaseRule {
    private String fieldName;
    private String expectedValue;
    private String expectedFieldName;

    public boolean evaluate(Map<String, Object> inputRecord) {
        if (StringUtils.isNotEmpty(expectedValue)) {
            return expectedValue.equals(String.valueOf(inputRecord.get(fieldName)));
        } else {
            return inputRecord.get(expectedFieldName).equals(inputRecord.get(fieldName));
        }
    }
}
