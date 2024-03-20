package cn.luixtech.frauddetection.simulator.domain;

import cn.luixtech.dae.common.command.Command;
import cn.luixtech.dae.common.rule.RuleCommand;
import cn.luixtech.dae.common.rule.RuleGroup;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Spring Data MongoDB collection for the DetectorRule entity.
 */
@Document
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Detector {
    @Id
    private String  id;
    private String  tenant;
    private Boolean enabled;

    private RuleGroup ruleGroup;

    public RuleCommand toRuleCommand() {
        RuleCommand ruleCommand = new RuleCommand();
        ruleCommand.setCreatedTime(System.currentTimeMillis());
        ruleCommand.setRuleGroup(ruleGroup);

        if (Boolean.TRUE.equals(this.enabled)) {
            ruleCommand.setCommand(Command.ADD);
        } else {
            ruleCommand.setCommand(Command.DELETE);
        }
        return ruleCommand;
    }
}
