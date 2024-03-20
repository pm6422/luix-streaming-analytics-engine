package cn.luixtech.dae.common.output;

import cn.luixtech.dae.common.input.Input;
import cn.luixtech.dae.common.rule.RuleGroup;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Output {
    private String    ruleGroupId;
    private RuleGroup violatedRuleGroup;
    private String    groupKeys;
    private Input     triggeringInput;
}
