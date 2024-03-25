package cn.luixtech.dae.common.rule.matching;

import lombok.Data;

@Data
public class MatchingRule {
    private String fieldName;
    /**
     * expectedValue and expectedFieldName can NOT have values at same time
     */
    private String expectedValue;
    /**
     * expectedValue and expectedFieldName can NOT have values at same time
     */
    private String expectedFieldName;
}
