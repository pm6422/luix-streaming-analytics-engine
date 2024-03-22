package cn.luixtech.dae.flinkjob.core;

import cn.luixtech.dae.common.command.Command;
import cn.luixtech.dae.common.input.Input;
import cn.luixtech.dae.common.output.Output;
import cn.luixtech.dae.common.rule.RuleCommand;
import cn.luixtech.dae.common.rule.RuleGroup;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Implements main rule evaluation and outputting logic.
 */
@Slf4j
public class RuleEvaluationFunction extends KeyedBroadcastProcessFunction<String, ShardingPolicy<Input, String, String>, RuleCommand, Output> {

    private              Meter                                outputMeter;
    private transient    MapState<Long, Set<Input>>           inputWindowState;
    private static final MapStateDescriptor<Long, Set<Input>> INPUT_WINDOW_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("inputWindowState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<>() {
            }));
    private static final String                               WIDEST_RULE_GROUP_KEY         = "widestRuleGroup";
    /**
     * TODO: handle threat-safe issue
     */
    private static final Map<String, Map<String, Long>>       LAST_MATCHING_TIME            = new HashMap<>();

    @Override
    public void open(Configuration parameters) {
        inputWindowState = getRuntimeContext().getMapState(INPUT_WINDOW_STATE_DESCRIPTOR);
        outputMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("outputsPerMinute", outputMeter);
    }

    @Override
    public void processBroadcastElement(RuleCommand ruleCommand, Context ctx, Collector<Output> out) throws Exception {
        log.debug("Received {}", ruleCommand);
        BroadcastState<String, RuleGroup> broadcastRuleGroupState = ctx.getBroadcastState(Descriptors.RULES_GROUP_DESCRIPTOR);
        // merge the new rule with the existing one
        RuleHelper.handleRuleCommand(broadcastRuleGroupState, ruleCommand);
        // update the rule group with the widest time window
        if (Command.ADD == ruleCommand.getCommand()) {
            updateWidestWindowRuleGroup(broadcastRuleGroupState, ruleCommand.getRuleGroup());
        }
    }

    /**
     * Evaluate rule group after receiving new input
     *
     * @param shardingPolicy The sharding policy
     * @param ctx            A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *                       querying the current processing/event time and iterating the broadcast state with
     *                       <b>read-only</b> access. The context is only valid during the invocation of this method,
     *                       do not store it.
     * @param out            The collector to emit resulting elements to
     * @throws Exception exception if any exception throws
     */
    @Override
    public void processElement(ShardingPolicy<Input, String, String> shardingPolicy, ReadOnlyContext ctx, Collector<Output> out) throws Exception {
        Input input = shardingPolicy.getInput();

        // store input to local map which is grouped by created time
        groupInputsByCreatedTime(inputWindowState, input.getCreatedTime(), input);

        // calculate handling latency time
        ctx.output(Descriptors.HANDLING_LATENCY_SINK_TAG, System.currentTimeMillis() - input.getIngestionTime());

        // get rule group by ID
        RuleGroup ruleGroup = ctx.getBroadcastState(Descriptors.RULES_GROUP_DESCRIPTOR).get(shardingPolicy.getRuleGroupId());
        if (ruleGroup == null) {
            log.error("No rule group: {}", shardingPolicy.getRuleGroupId());
            return;
        }

        // truncate the milliseconds part of the created time value
        long cleanupTime = (input.getCreatedTime() / 1000) * 1000;
        // register cleanup timer
        ctx.timerService().registerEventTimeTimer(cleanupTime);

        // evaluate the rule and trigger an output if matched
        boolean ruleMatched = RuleHelper.evaluateRuleGroup(ruleGroup, input, inputWindowState);

        // output rule evaluation result
        ctx.output(Descriptors.RULE_EVALUATION_RESULT_TAG,
                "Rule group: " + ruleGroup.getId() + " , sharding key: " + shardingPolicy.getShardingKey() + " , matched: " + ruleMatched);

        if (ruleMatched && isAfterSilentPeriod(ruleGroup, input)) {
            if (ruleGroup.isResetAfterMatch()) {
                evictAllInputs();
            }
            outputMeter.markEvent();

            long matchingTime = System.currentTimeMillis();
            if (!LAST_MATCHING_TIME.containsKey(ruleGroup.getId())) {
                LAST_MATCHING_TIME.put(ruleGroup.getId(), new HashMap<>());
            }
            LAST_MATCHING_TIME.get(ruleGroup.getId()).put(input.getEntityId(), matchingTime);
            out.collect(new Output(ruleGroup.getId(), ruleGroup, shardingPolicy.getShardingKey(), shardingPolicy.getInput(), matchingTime));
        }
    }

    private void updateWidestWindowRuleGroup(BroadcastState<String, RuleGroup> broadcastRuleCommandState, RuleGroup newRuleGroup) throws Exception {
        RuleGroup widestWindowRule = broadcastRuleCommandState.get(WIDEST_RULE_GROUP_KEY);
        if (widestWindowRule == null) {
            broadcastRuleCommandState.put(WIDEST_RULE_GROUP_KEY, newRuleGroup);
            return;
        }
        if (newRuleGroup.getWindowMinutes() > widestWindowRule.getWindowMinutes()) {
            broadcastRuleCommandState.put(WIDEST_RULE_GROUP_KEY, newRuleGroup);
        }
    }

    private static <K, V> void groupInputsByCreatedTime(MapState<K, Set<V>> inputWindowState, K key, V value) throws Exception {
        Set<V> valuesSet = inputWindowState.get(key);
        if (valuesSet == null) {
            valuesSet = new HashSet<>();
        }
        valuesSet.add(value);
        inputWindowState.put(key, valuesSet);
    }

    @Override
    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Output> out) throws Exception {
        RuleGroup widestWindowRuleGroup = ctx.getBroadcastState(Descriptors.RULES_GROUP_DESCRIPTOR).get(WIDEST_RULE_GROUP_KEY);
        if (widestWindowRuleGroup == null) {
            return;
        }
        long widestTimeWindowInMillis = TimeUnit.MINUTES.toMillis(widestWindowRuleGroup.getWindowMinutes());
        long cleanupTimeThreshold = timestamp - widestTimeWindowInMillis;
        evictAgedInputsFromWindow(cleanupTimeThreshold);
    }

    private void evictAgedInputsFromWindow(Long threshold) {
        try {
            Iterator<Long> inputCreatedTimeIterator = inputWindowState.keys().iterator();
            while (inputCreatedTimeIterator.hasNext()) {
                Long inputCreatedTime = inputCreatedTimeIterator.next();
                if (inputCreatedTime < threshold) {
                    // remove aged input
                    inputCreatedTimeIterator.remove();
                }
            }
        } catch (Exception ex) {
            log.error("Failed to evict the aged inputs");
        }
    }

    private void evictAllInputs() {
        try {
            Iterator<Long> inputCreatedTimeIterator = inputWindowState.keys().iterator();
            while (inputCreatedTimeIterator.hasNext()) {
                inputCreatedTimeIterator.next();
                inputCreatedTimeIterator.remove();
            }
        } catch (Exception ex) {
            log.error("Failed to evict all inputs");
        }
    }

    private boolean isAfterSilentPeriod(RuleGroup ruleGroup, Input input) {
        if (!LAST_MATCHING_TIME.containsKey(ruleGroup.getId())
                || !LAST_MATCHING_TIME.get(ruleGroup.getId()).containsKey(input.getEntityId())
                || ruleGroup.getSilentMinutes() == 0) {
            return true;
        }
        return System.currentTimeMillis() >
                LAST_MATCHING_TIME.get(ruleGroup.getId()).get(input.getEntityId()) + TimeUnit.MINUTES.toMillis(ruleGroup.getSilentMinutes());
    }
}
