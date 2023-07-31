package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.dto.Alert;
import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.common.rule.ControlType;
import com.luixtech.frauddetection.common.rule.RuleState;
import com.luixtech.frauddetection.flinkjob.utils.FieldsExtractor;
import com.luixtech.frauddetection.flinkjob.utils.ProcessingUtils;
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
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.luixtech.frauddetection.flinkjob.utils.ProcessingUtils.addToStateValuesSet;

/**
 * Implements main rule evaluation and alerting logic.
 */
@Slf4j
public class DynamicAlertFunction extends KeyedBroadcastProcessFunction<String, Keyed<Transaction, String, Integer>, Rule, Alert> {

    private static final String                                     COUNT                   = "COUNT_FLINK";
    private static final String                                     COUNT_WITH_RESET        = "COUNT_WITH_RESET_FLINK";
    private static final int                                        WIDEST_RULE_KEY         = Integer.MIN_VALUE;
    private static final int                                        CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;
    private transient    MapState<Long, Set<Transaction>>           windowState;
    private              Meter                                      alertMeter;
    private static final MapStateDescriptor<Long, Set<Transaction>> WINDOW_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "windowState",
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeInformation.of(new TypeHint<>() {
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
            handleControlCommand(rule.getControlType(), broadcastState, ctx);
        }
    }

    private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState) throws Exception {
        Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
        if (rule.getRuleState() != RuleState.ACTIVE) {
            return;
        }
        if (widestWindowRule == null) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
            return;
        }

        if (TimeUnit.MINUTES.toMillis(widestWindowRule.getWindowMinutes()) < TimeUnit.MINUTES.toMillis(rule.getWindowMinutes())) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
        }
    }

    private void handleControlCommand(ControlType controlType, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
        switch (controlType) {
            case CLEAR_ALL_STATE:
                ctx.applyToKeyedState(WINDOW_STATE_DESCRIPTOR, (key, state) -> state.clear());
                break;
            case CLEAR_STATE_ALL_STOP:
                rulesState.remove(CLEAR_STATE_COMMAND_KEY);
                break;
        }
    }

    @Override
    public void processElement(Keyed<Transaction, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
        Transaction transaction = value.getWrapped();
        long eventTime = transaction.getEventTime();
        // Add Transaction to state
        addToStateValuesSet(windowState, eventTime, transaction);

        ctx.output(Descriptors.LATENCY_SINK_TAG, System.currentTimeMillis() - transaction.getIngestionTimestamp());

        // Get rule by ID
        Rule rule = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR).get(value.getId());
        if (rule == null) {
            log.error("Rule with ID [{}] does not exist", value.getId());
            return;
        }

        if (rule.getRuleState() == RuleState.ACTIVE) {
            Long windowStartForEvent = eventTime - TimeUnit.MINUTES.toMillis(rule.getWindowMinutes());

            long cleanupTime = (eventTime / 1000) * 1000;
            ctx.timerService().registerEventTimeTimer(cleanupTime);

            // Calculate the aggregate value
            SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
            for (Long stateEventTime : windowState.keys()) {
                if (isStateValueInWindow(stateEventTime, windowStartForEvent, eventTime)) {
                    aggregateValuesInState(stateEventTime, aggregator, rule);
                }
            }

            BigDecimal aggregateResult = aggregator.getLocalValue();
            // Evaluate the rule and trigger an alert if violated
            boolean ruleMatched = rule.apply(aggregateResult);

            ctx.output(Descriptors.DEMO_SINK_TAG,
                    "Rule "
                            + rule.getRuleId()
                            + " | "
                            + value.getKey()
                            + " : "
                            + aggregateResult.toString()
                            + " -> "
                            + ruleMatched);

            if (ruleMatched) {
                if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
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

    private void aggregateValuesInState(Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
        Set<Transaction> inWindow = windowState.get(stateEventTime);
        if (COUNT.equals(rule.getAggregateFieldName()) || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
            for (Transaction event : inWindow) {
                aggregator.add(BigDecimal.ONE);
            }
        } else {
            for (Transaction event : inWindow) {
                BigDecimal aggregatedValue = FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), event);
                aggregator.add(aggregatedValue);
            }
        }
    }

    @Override
    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out) throws Exception {
        Rule widestWindowRule = ctx.getBroadcastState(Descriptors.RULES_DESCRIPTOR).get(WIDEST_RULE_KEY);

        Optional<Long> cleanupEventTimeWindow = Optional.ofNullable(widestWindowRule).map(rule -> TimeUnit.MINUTES.toMillis(rule.getWindowMinutes()));
        Optional<Long> cleanupEventTimeThreshold = cleanupEventTimeWindow.map(window -> timestamp - window);
        cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
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
