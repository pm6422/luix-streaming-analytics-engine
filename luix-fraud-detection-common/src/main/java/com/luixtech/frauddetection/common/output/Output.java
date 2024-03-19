package com.luixtech.frauddetection.common.output;

import com.luixtech.frauddetection.common.rule.RuleGroup;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Output<E> {
    private String    ruleId;
    private RuleGroup violatedRuleGroup;
    private String    groupKeys;
    private E      triggeringInput;
}
