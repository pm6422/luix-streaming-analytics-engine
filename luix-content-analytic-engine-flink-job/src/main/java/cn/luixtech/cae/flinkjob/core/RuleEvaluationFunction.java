package cn.luixtech.cae.flinkjob.core;

import cn.luixtech.cae.common.output.Output;
import cn.luixtech.cae.common.command.Command;
import cn.luixtech.cae.common.input.Input;
import cn.luixtech.cae.common.rule.RuleGroup;
import cn.luixtech.cae.common.rule.RuleCommand;
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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
    private static final String                               WIDEST_RULE_COMMAND_KEY       = "widestRuleCommand";

    @Override
    public void open(Configuration parameters) {
        inputWindowState = getRuntimeContext().getMapState(INPUT_WINDOW_STATE_DESCRIPTOR);
        outputMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("outputsPerMinute", outputMeter);
    }

    @Override
    public void processBroadcastElement(RuleCommand ruleCommand, Context ctx, Collector<Output> out) throws Exception {
        log.debug("Received {}", ruleCommand);
        BroadcastState<String, RuleCommand> broadcastRuleCommandState = ctx.getBroadcastState(Descriptors.RULES_COMMAND_DESCRIPTOR);
        // merge the new rule with the existing one
        RuleHelper.handleRuleCommand(broadcastRuleCommandState, ruleCommand);
        // update the rule command with the widest time window
        updateWidestWindowRuleCommand(broadcastRuleCommandState, ruleCommand);
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

        // get rule command by ID
        RuleCommand ruleCommand = ctx.getBroadcastState(Descriptors.RULES_COMMAND_DESCRIPTOR).get(shardingPolicy.getRuleGroupId());
        if (ruleCommand == null) {
            log.error("Rule command [{}] does not exist", shardingPolicy.getRuleGroupId());
            return;
        }
        if (Command.ADD != ruleCommand.getCommand()) {
            log.warn("Rule command [{}] is inactive", shardingPolicy.getRuleGroupId());
            return;
        }

        // truncate the milliseconds part of the created time value
        long cleanupTime = (input.getCreatedTime() / 1000) * 1000;
        // register cleanup timer
        ctx.timerService().registerEventTimeTimer(cleanupTime);

        RuleGroup ruleGroup = ruleCommand.getRuleGroup();

        // evaluate the rule and trigger an output if matched
        boolean ruleMatched = RuleHelper.evaluateRuleGroup(ruleGroup, input, inputWindowState);

        // output rule evaluation result
        ctx.output(Descriptors.RULE_EVALUATION_RESULT_TAG,
                "Rule group: " + ruleGroup.getId() + " , sharding key: " + shardingPolicy.getShardingKey() + " , matched: " + ruleMatched);

        if (ruleMatched) {
            if (ruleCommand.getRuleGroup().isResetAfterMatch()) {
                evictAllInputs();
            }
            outputMeter.markEvent();
            out.collect(new Output(ruleGroup.getId(), ruleGroup, shardingPolicy.getShardingKey(), shardingPolicy.getInput()));
        }
    }

    private void updateWidestWindowRuleCommand(BroadcastState<String, RuleCommand> broadcastRuleCommandState, RuleCommand ruleCommand) throws Exception {
        RuleCommand widestWindowRule = broadcastRuleCommandState.get(WIDEST_RULE_COMMAND_KEY);
        if (Command.ADD != ruleCommand.getCommand()) {
            return;
        }
        if (widestWindowRule == null) {
            broadcastRuleCommandState.put(WIDEST_RULE_COMMAND_KEY, ruleCommand);
            return;
        }
        if (ruleCommand.getRuleGroup().getWindowMinutes() > widestWindowRule.getRuleGroup().getWindowMinutes()) {
            broadcastRuleCommandState.put(WIDEST_RULE_COMMAND_KEY, ruleCommand);
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
        RuleCommand widestWindowRuleCommand = ctx.getBroadcastState(Descriptors.RULES_COMMAND_DESCRIPTOR).get(WIDEST_RULE_COMMAND_KEY);
        if (widestWindowRuleCommand == null) {
            return;
        }
        long widestTimeWindowInMillis = TimeUnit.MINUTES.toMillis(widestWindowRuleCommand.getRuleGroup().getWindowMinutes());
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
}
