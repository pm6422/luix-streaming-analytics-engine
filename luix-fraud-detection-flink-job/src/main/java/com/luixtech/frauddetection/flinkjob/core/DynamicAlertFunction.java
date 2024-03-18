package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.alert.Alert;
import com.luixtech.frauddetection.common.command.Command;
import com.luixtech.frauddetection.common.input.InputRecord;
import com.luixtech.frauddetection.common.rule.Rule;
import com.luixtech.frauddetection.common.rule.RuleCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
 * Implements main rule evaluation and alerting logic.
 */
@Slf4j
public class DynamicAlertFunction extends KeyedBroadcastProcessFunction<String, Keyed<InputRecord, String, String>, RuleCommand, Alert> {

    private static final String                                     WIDEST_RULE_KEY         = StringUtils.EMPTY + Integer.MIN_VALUE;
    private              Meter                                      alertMeter;
    private transient    MapState<Long, Set<InputRecord>>           windowState;
    private static final MapStateDescriptor<Long, Set<InputRecord>> WINDOW_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("windowState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<>() {
            }));

    @Override
    public void open(Configuration parameters) {
        windowState = getRuntimeContext().getMapState(WINDOW_STATE_DESCRIPTOR);
        alertMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
    }

    @Override
    public void processBroadcastElement(RuleCommand ruleCommand, Context ctx, Collector<Alert> out) throws Exception {
        log.debug("Received {}", ruleCommand);
        BroadcastState<String, RuleCommand> broadcastState = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR);
        // Merge the new rule with the existing one
        RuleHelper.handleRule(broadcastState, ruleCommand);
        updateWidestWindowRule(ruleCommand, broadcastState);
    }

    private void updateWidestWindowRule(RuleCommand ruleCommand, BroadcastState<String, RuleCommand> broadcastState) throws Exception {
        RuleCommand widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
        if (Command.ADD != ruleCommand.getCommand()) {
            return;
        }
        if (widestWindowRule == null) {
            broadcastState.put(WIDEST_RULE_KEY, ruleCommand);
            return;
        }
        if (ruleCommand.getRule().getWindowMinutes() > widestWindowRule.getRule().getWindowMinutes()) {
            broadcastState.put(WIDEST_RULE_KEY, ruleCommand);
        }
    }

    /**
     * Called for each element after received rule
     *
     * @param value The stream element.
     * @param ctx   A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *              querying the current processing/event time and iterating the broadcast state with
     *              <b>read-only</b> access. The context is only valid during the invocation of this method,
     *              do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception exception
     */
    @Override
    public void processElement(Keyed<InputRecord, String, String> value, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
        InputRecord transaction = value.getWrapped();
        long createdTime = transaction.getCreatedTime();

        // Store transaction to local map which is grouped by created time
        groupTransactionByTime(windowState, createdTime, transaction);

        // Calculate handling latency time
        ctx.output(Descriptors.HANDLING_LATENCY_SINK_TAG, System.currentTimeMillis() - transaction.getIngestionTime());

        // Get rule command by ID
        RuleCommand ruleCommand = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR).get(value.getId());
        if (ruleCommand == null) {
            log.error("Rule [{}] does not exist", value.getId());
            return;
        }

        if (Command.ADD != ruleCommand.getCommand()) {
            // Do NOT handle delete command
            return;
        }

        long cleanupTime = (createdTime / 1000) * 1000;
        // Register cleanup timer
        ctx.timerService().registerEventTimeTimer(cleanupTime);

        Rule rule = ruleCommand.getRule();

        // Evaluate the rule and trigger an alert if matched
        boolean ruleMatched = RuleHelper.evaluate(rule, transaction, windowState);

        // Print rule evaluation result
        ctx.output(Descriptors.RULE_EVALUATION_RESULT_TAG,
                "Rule: " + rule.getId() + " , Keys: " + value.getKey() + " , Matched: " + ruleMatched);

        if (ruleMatched) {
            if (ruleCommand.getRule().isResetAfterMatch()) {
                evictAllStateElements();
            }
            alertMeter.markEvent();
            out.collect(new Alert<>(rule.getId(), rule, value.getKey(), value.getWrapped()));
        }
    }

    private static <K, V> void groupTransactionByTime(MapState<K, Set<V>> mapState, K key, V value) throws Exception {
        Set<V> valuesSet = mapState.get(key);
        if (valuesSet == null) {
            valuesSet = new HashSet<>();
        }
        valuesSet.add(value);
        mapState.put(key, valuesSet);
    }

    @Override
    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out) throws Exception {
        RuleCommand widestWindowRule = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR).get(WIDEST_RULE_KEY);
        if (widestWindowRule == null) {
            return;
        }
        long cleanupEventTimeWindow = TimeUnit.MINUTES.toMillis(widestWindowRule.getRule().getWindowMinutes());
        long cleanupEventTimeThreshold = timestamp - cleanupEventTimeWindow;
        evictAgedElementsFromWindow(cleanupEventTimeThreshold);
    }

    private void evictAgedElementsFromWindow(Long threshold) {
        try {
            Iterator<Long> keys = windowState.keys().iterator();
            while (keys.hasNext()) {
                Long stateEventTime = keys.next();
                if (stateEventTime < threshold) {
                    keys.remove();
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void evictAllStateElements() {
        try {
            Iterator<Long> keys = windowState.keys().iterator();
            while (keys.hasNext()) {
                keys.next();
                keys.remove();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
