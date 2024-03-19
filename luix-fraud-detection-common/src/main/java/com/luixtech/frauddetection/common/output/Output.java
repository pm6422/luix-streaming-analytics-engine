package com.luixtech.frauddetection.common.output;

import com.luixtech.frauddetection.common.rule.Rule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Output<E> {
    private String ruleId;
    private Rule   violatedRule;
    private String groupKeys;
    private E      triggeringInput;
}
