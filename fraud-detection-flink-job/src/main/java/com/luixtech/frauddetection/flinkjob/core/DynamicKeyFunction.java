package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.flinkjob.utils.KeysExtractor;
import com.luixtech.frauddetection.flinkjob.utils.ProcessingUtils;
import lombok.Data;
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
public class DynamicKeyFunction extends BroadcastProcessFunction<Transaction, Rule, Keyed<Transaction, String, Integer>> {
    private RuleCounterGauge ruleCounterGauge;

    @Override
    public void open(Configuration parameters) {
        ruleCounterGauge = new RuleCounterGauge();
        getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Keyed<Transaction, String, Integer>> out) throws Exception {
        log.debug("Received {}", rule);
        BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR);
        // Merge the new rule with the existing one
        ProcessingUtils.handleRule(broadcastState, rule);
    }

    @Override
    public void processElement(Transaction transaction, ReadOnlyContext ctx, Collector<Keyed<Transaction, String, Integer>> out) throws Exception {
        ReadOnlyBroadcastState<Integer, Rule> rulesState = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR);
        int ruleCounter = 0;
        for (Map.Entry<Integer, Rule> entry : rulesState.immutableEntries()) {
            final Rule rule = entry.getValue();
            out.collect(new Keyed<>(transaction, KeysExtractor.toKeys(transaction, rule.getGroupingKeyNames()), rule.getRuleId()));
            ruleCounter++;
        }
        ruleCounterGauge.setValue(ruleCounter);
    }

    @Data
    private static class RuleCounterGauge implements Gauge<Integer> {
        private int value = 0;

        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public Integer getValue() {
            return value;
        }
    }
}
