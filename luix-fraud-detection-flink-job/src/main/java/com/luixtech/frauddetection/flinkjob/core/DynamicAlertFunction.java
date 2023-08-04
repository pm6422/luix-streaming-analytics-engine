package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.dto.Alert;
import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.common.rule.ControlType;
import com.luixtech.frauddetection.common.rule.RuleState;
import com.luixtech.frauddetection.flinkjob.utils.FieldsExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
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

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implements main rule evaluation and alerting logic.
 */
@Slf4j
public class DynamicAlertFunction extends KeyedBroadcastProcessFunction<String, Keyed<Transaction, Integer, String>, Rule, Alert> {

    private static final int                                        WIDEST_RULE_KEY         = Integer.MIN_VALUE;
    private              Meter                                      alertMeter;
    private transient    MapState<Long, Set<Transaction>>           windowState;
    private static final MapStateDescriptor<Long, Set<Transaction>> WINDOW_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("windowState", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<>() {
            }));

    @Override
    public void open(Configuration parameters) {
        windowState = getRuntimeContext().getMapState(WINDOW_STATE_DESCRIPTOR);
        alertMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out) throws Exception {
        log.debug("Received {}", rule);
        BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR);
        // Merge the new rule with the existing one
        RuleHelper.handleRule(broadcastState, rule);
        updateWidestWindowRule(rule, broadcastState);
        if (rule.getRuleState() == RuleState.CONTROL) {
            handleControlCommand(rule.getControlType(), ctx);
        }
    }

    private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState) throws Exception {
        Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
        if (RuleState.ACTIVE != rule.getRuleState()) {
            return;
        }
        if (widestWindowRule == null) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
            return;
        }
        if (rule.getWindowMinutes() > widestWindowRule.getWindowMinutes()) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
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
     * @throws Exception
     */
    @Override
    public void processElement(Keyed<Transaction, Integer, String> value, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
        Transaction transaction = value.getWrapped();
        long eventTime = transaction.getEventTime();
        // Store transaction to local map which is grouped by event time
        groupTransactionByTime(windowState, eventTime, transaction);

        // Calculate handling latency time
        ctx.output(Descriptors.HANDLING_LATENCY_SINK_TAG, System.currentTimeMillis() - transaction.getIngestionTimestamp());

        // Get rule by ID
        Rule rule = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR).get(value.getId());
        if (rule == null) {
            log.error("Rule with ID [{}] does not exist", value.getId());
            return;
        }

        if (rule.getRuleState() == RuleState.ACTIVE) {
            long cleanupTime = (eventTime / 1000) * 1000;
            // Register cleanup timer
            ctx.timerService().registerEventTimeTimer(cleanupTime);

            Long windowStartForEvent = eventTime - TimeUnit.MINUTES.toMillis(rule.getWindowMinutes());

            // Calculate the aggregate value
            SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
            for (Long stateEventTime : windowState.keys()) {
                if (isStateValueInWindow(stateEventTime, windowStartForEvent, eventTime)) {
                    Set<Transaction> inWindow = windowState.get(stateEventTime);
                    for (Transaction t : inWindow) {
                        BigDecimal aggregatedValue = FieldsExtractor.getBigDecimalByName(t, rule.getAggregateFieldName());
                        aggregator.add(aggregatedValue);
                    }
                }
            }

            BigDecimal aggregateResult = aggregator.getLocalValue();
            // Evaluate the rule and trigger an alert if violated
            boolean ruleViolated = rule.apply(aggregateResult);

            // Print rule evaluation result
            ctx.output(Descriptors.RULE_EVALUATION_RESULT_TAG,
                    "Rule: " + rule.getRuleId() + " | Keys: " + value.getKey() + " | Aggregate Result: " + aggregateResult.toString() + " | Matched: " + ruleViolated);

            if (ruleViolated) {
                if (rule.isResetAfterMatch()) {
                    evictAllStateElements();
                }
                alertMeter.markEvent();
                out.collect(new Alert<>(rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
            }
        }
    }

    private boolean isStateValueInWindow(Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
        return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
    }

    private static <K, V> Set<V> groupTransactionByTime(MapState<K, Set<V>> mapState, K key, V value) throws Exception {
        Set<V> valuesSet = mapState.get(key);
        if (valuesSet == null) {
            valuesSet = new HashSet<>();
        }
        valuesSet.add(value);
        mapState.put(key, valuesSet);
        return valuesSet;
    }

    @Override
    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out) throws Exception {
        Rule widestWindowRule = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR).get(WIDEST_RULE_KEY);
        if (widestWindowRule == null) {
            return;
        }
        long cleanupEventTimeWindow = TimeUnit.MINUTES.toMillis(widestWindowRule.getWindowMinutes());
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

    private void handleControlCommand(ControlType controlType, Context ctx) throws Exception {
        switch (controlType) {
            case CLEAR_ALL_STATE:
                ctx.applyToKeyedState(WINDOW_STATE_DESCRIPTOR, (key, state) -> state.clear());
                break;
        }
    }
}
