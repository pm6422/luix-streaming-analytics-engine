package com.luixtech.frauddetection.common.rule.matching;

import lombok.Data;

@Data
public class MatchingRule {
    private String fieldName;
    private String expectedValue;
    private String expectedFieldName;
}
