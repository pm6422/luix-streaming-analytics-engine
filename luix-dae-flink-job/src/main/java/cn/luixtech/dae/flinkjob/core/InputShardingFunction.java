package cn.luixtech.dae.flinkjob.core;

import cn.luixtech.dae.common.input.Input;
import cn.luixtech.dae.common.rule.RuleCommand;
import cn.luixtech.dae.common.rule.RuleGroup;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Implements dynamic data partitioning based on a set of broadcast rules.
 */
@Slf4j
public class InputShardingFunction extends BroadcastProcessFunction<Input, RuleCommand, ShardingPolicy<Input, String, String>> {
    private RuleGroupCounterGauge ruleGroupCounterGauge;

    @Override
    public void open(Configuration parameters) {
        ruleGroupCounterGauge = new RuleGroupCounterGauge();
        getRuntimeContext().getMetricGroup().gauge("activeRuleGroupCount", ruleGroupCounterGauge);
    }

    /**
     * Handle rule command while the new rule come
     *
     * @param ruleCommand The rule command.
     * @param ctx         A {@link Context} that allows querying the timestamp of the element, querying the
     *                    current processing/event time and updating the broadcast state. The context is only valid
     *                    during the invocation of this method, do not store it.
     * @param out         The collector to emit resulting elements to
     * @throws Exception if any exceptions throw
     */
    @Override
    public void processBroadcastElement(RuleCommand ruleCommand, Context ctx, Collector<ShardingPolicy<Input, String, String>> out) throws Exception {
        log.debug("Received {}", ruleCommand);
        BroadcastState<String, RuleGroup> broadcastRuleCommandState = ctx.getBroadcastState(Descriptors.RULES_GROUP_DESCRIPTOR);
        // merge the new rule with the existing one
        RuleHelper.handleRuleCommand(broadcastRuleCommandState, ruleCommand);
    }

    /**
     * Handle input while the new input come
     *
     * @param input The stream element.
     * @param ctx   A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *              querying the current processing/event time and updating the broadcast state. The context
     *              is only valid during the invocation of this method, do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception if any exceptions throw
     */
    @Override
    public void processElement(Input input, ReadOnlyContext ctx, Collector<ShardingPolicy<Input, String, String>> out) throws Exception {
        ReadOnlyBroadcastState<String, RuleGroup> ruleCommandState = ctx.getBroadcastState(Descriptors.RULES_GROUP_DESCRIPTOR);
        int ruleGroupCounter = 0;
        // iterate all rule groups and evaluate it for all inputs
        for (Map.Entry<String, RuleGroup> entry : ruleCommandState.immutableEntries()) {
            final RuleGroup ruleGroup = entry.getValue();
            // Combines groupingValues as a single concatenated key, e.g "{tenant=tesla, model=X9}".
            // Flink will calculate the hash of this sharding key and assign the processing of this particular combination
            // to a specific server in the cluster. That is to say, inputs with the same sharding key are assigned to the same partition.
            out.collect(new ShardingPolicy<>(input, input.getGroupingValues().toString(), ruleGroup.getId()));
            ruleGroupCounter++;
        }
        ruleGroupCounterGauge.setValue(ruleGroupCounter);
    }

    @Setter
    @Data
    private static class RuleGroupCounterGauge implements Gauge<Integer> {
        private int value = 0;

        @Override
        public Integer getValue() {
            return value;
        }
    }
}
