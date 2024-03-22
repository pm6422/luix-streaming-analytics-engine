package cn.luixtech.dae.flinkjob.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShardingPolicy<IN, KEY, ID> {
    /**
     * Input
     */
    private IN  input;
    /**
     * Sharding key used to partition the input, e.g: {tenant=tesla, model=X9}
     */
    private KEY shardingKey;
    /**
     * The id of the rule group
     */
    private ID  ruleGroupId;
}
