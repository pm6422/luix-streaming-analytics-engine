package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.input.Input;
import com.luixtech.frauddetection.common.rule.Rule;
import com.luixtech.frauddetection.common.rule.RuleCommand;
import com.luixtech.frauddetection.common.rule.RuleType;
import com.luixtech.frauddetection.flinkjob.core.accumulator.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Collection of helper methods for Rules.
 */
@Slf4j
public class RuleHelper {
    public static void handleRule(BroadcastState<String, RuleCommand> broadcastState, RuleCommand ruleCommand) throws Exception {
        switch (ruleCommand.getCommand()) {
            case ADD:
                // merge rule by ID
                broadcastState.put(ruleCommand.getRule().getId(), ruleCommand);
                break;
            case DELETE:
                broadcastState.remove(ruleCommand.getRule().getId());
                break;
            case DELETE_ALL:
                Iterator<Map.Entry<String, RuleCommand>> entriesIterator = broadcastState.iterator();
                while (entriesIterator.hasNext()) {
                    Map.Entry<String, RuleCommand> ruleEntry = entriesIterator.next();
                    broadcastState.remove(ruleEntry.getKey());
                    log.info("Removed {}", ruleEntry.getValue());
                }
                break;
        }
    }

    /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        switch (rule.getAggregator()) {
            case COUNT:
                return new BigDecimalCounter();
            case SUM:
                return new BigDecimalAdder();
            case AVG:
                return new BigDecimalAverageAccumulator();
            case MAX:
                return new BigDecimalMaximum();
            case MIN:
                return new BigDecimalMinimum();
            default:
                throw new RuntimeException(
                        "Unsupported aggregation function type: " + rule.getAggregator());
        }
    }

    public static boolean evaluate(Rule rule, Input input,
                                   MapState<Long, Set<Input>> windowState) throws Exception {
        return RuleType.MATCHING == rule.determineType()
                ? evaluateMatchingRule(rule, input)
                : evaluateAggregatingRule(rule, input, windowState);
    }

    /**
     * Evaluates matching rule by comparing field value with expected value based on operator.
     *
     * @param rule  matching rule to evaluate
     * @param input inputInWindow data
     * @return true if matched, otherwise false
     */
    private static boolean evaluateMatchingRule(Rule rule, Input input) {
        if (StringUtils.isNotEmpty(rule.getExpectedValue())) {
            return rule.getExpectedValue().equals(input.getRecord().get(rule.getFieldName()).toString());
        }
        return input.getRecord().get(rule.getFieldName()).equals(input.getRecord().get(rule.getExpectedFieldName()));
    }

    /**
     * Evaluates aggregate rule by comparing provided value with rules' limit based on operator.
     *
     * @param rule        aggregation rule to evaluate
     * @param input inputInWindow data
     * @param windowState inputInWindow data group by time window
     * @return true if matched, otherwise false
     * @throws Exception if exception throws
     */
    private static boolean evaluateAggregatingRule(Rule rule, Input input,
                                                   MapState<Long, Set<Input>> windowState) throws Exception {
        Long windowStartTime = input.getCreatedTime() - TimeUnit.MINUTES.toMillis(rule.getWindowMinutes());

        // Calculate the aggregate value
        SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
        for (Long stateCreatedTime : windowState.keys()) {
            if (isStateValueInWindow(stateCreatedTime, windowStartTime, input.getCreatedTime())) {
                Set<Input> inputsInWindow = windowState.get(stateCreatedTime);
                for (Input inputInWindow : inputsInWindow) {
                    BigDecimal aggregatedValue = getBigDecimalByFieldName(inputInWindow.getRecord(), rule.getAggregateFieldName());
                    aggregator.add(aggregatedValue);
                }
            }
        }
        BigDecimal actualAggregatedValue = aggregator.getLocalValue();
        rule.setActualAggregatedValue(actualAggregatedValue);

        // compare the expected value with the actual one based on operator
        return rule.getOperator().compare(actualAggregatedValue, rule.getExpectedLimitValue());
    }

    private static boolean isStateValueInWindow(Long stateCreatedTime, Long windowStartTime, long currentEventTime) {
        return stateCreatedTime >= windowStartTime && stateCreatedTime <= currentEventTime;
    }

    private static BigDecimal getBigDecimalByFieldName(Map<String, Object> record, String fieldName) {
        if (StringUtils.isEmpty(fieldName)) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(record.get(fieldName).toString());
    }
}
