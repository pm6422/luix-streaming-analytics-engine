package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.input.Input;
import com.luixtech.frauddetection.common.rule.RuleCommand;
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
public class DynamicKeyFunction extends BroadcastProcessFunction<Input, RuleCommand, ShardingPolicy<Input, String, String>> {
    private RuleCounterGauge ruleCounterGauge;

    @Override
    public void open(Configuration parameters) {
        ruleCounterGauge = new RuleCounterGauge();
        getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
    }

    @Override
    public void processBroadcastElement(RuleCommand ruleCommand, Context ctx, Collector<ShardingPolicy<Input, String, String>> out) throws Exception {
        log.debug("Received {}", ruleCommand);
        BroadcastState<String, RuleCommand> broadcastState = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR);
        // Merge the new rule with the existing one
        RuleHelper.handleRule(broadcastState, ruleCommand);
    }

    @Override
    public void processElement(Input input, ReadOnlyContext ctx, Collector<ShardingPolicy<Input, String, String>> out) throws Exception {
        ReadOnlyBroadcastState<String, RuleCommand> rulesState = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR);
        int ruleCounter = 0;
        for (Map.Entry<String, RuleCommand> entry : rulesState.immutableEntries()) {
            final RuleCommand ruleCommand = entry.getValue();
            // Combines groupingValues as a single concatenated key, e.g "{tenant=tesla, model=X9}".
            // Flink will calculate the hash of this key and assign the processing of this particular combination to a specific server
            // in the cluster. That is to say, inputs with the same key are assigned to the same partition.
            // This will allow tracking all input between tenant #tesla and model #X9 and iterate all rules and evaluate it to all inputs
            out.collect(new ShardingPolicy<>(input, input.getGroupingValues().toString(), ruleCommand.getRuleGroup().getId()));
            ruleCounter++;
        }
        ruleCounterGauge.setValue(ruleCounter);
    }

    @Setter
    @Data
    private static class RuleCounterGauge implements Gauge<Integer> {
        private int value = 0;

        @Override
        public Integer getValue() {
            return value;
        }
    }
}
