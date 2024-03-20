package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.input.Input;
import com.luixtech.frauddetection.common.rule.*;
import com.luixtech.frauddetection.common.rule.aggregating.Aggregator;
import com.luixtech.frauddetection.flinkjob.core.accumulator.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
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
                broadcastState.put(ruleCommand.getRuleGroup().getId(), ruleCommand);
                break;
            case DELETE:
                broadcastState.remove(ruleCommand.getRuleGroup().getId());
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

    /**
     * Picks and returns a new accumulator instance, based on the Rule's aggregator
     *
     * @param aggregator aggregator of rule
     * @return aggregator instance
     */
    public static SimpleAccumulator<BigDecimal> getAggregator(Aggregator aggregator) {
        switch (aggregator) {
            case COUNT:
                return new BigDecimalCountAccumulator();
            case SUM:
                return new BigDecimalSumAccumulator();
            case AVERAGE:
                return new BigDecimalAverageAccumulator();
            case MAX:
                return new BigDecimalMaxAccumulator();
            case MIN:
                return new BigDecimalMinAccumulator();
            default:
                throw new IllegalArgumentException("Unsupported aggregator: " + aggregator);
        }
    }

    public static boolean evaluateRuleGroup(RuleGroup ruleGroup, Input input, MapState<Long, Set<Input>> windowState) throws Exception {
        if (ruleGroup == null) {
            return false;
        }
        if (StringUtils.isNotEmpty(ruleGroup.getTenant()) && !ruleGroup.getTenant().equals(input.getTenant())) {
            // return false if existing tenant value of rule is not identical to the one of input object
            return false;
        }
        boolean result = ruleGroup.getLogicalOperator() == LogicalOperator.AND;

        // Evaluate child rule groups
        if (CollectionUtils.isNotEmpty(ruleGroup.getChildren())) {
            for (RuleGroup childGroup : ruleGroup.getChildren()) {
                boolean childResult = evaluateRuleGroup(childGroup, input, windowState);
                if (ruleGroup.getLogicalOperator() == LogicalOperator.AND) {
                    result = result && childResult;
                } else {
                    result = result || childResult;
                }
            }
        }

        // Evaluate rules
        if (CollectionUtils.isNotEmpty(ruleGroup.getRules())) {
            for (Rule rule : ruleGroup.getRules()) {
                boolean ruleResult = evaluateRule(rule, input, windowState);
                if (ruleGroup.getLogicalOperator() == LogicalOperator.AND) {
                    result = result && ruleResult;
                } else {
                    result = result || ruleResult;
                }
            }
        }
        return result;
    }

    public static boolean evaluateRule(Rule rule, Input input, MapState<Long, Set<Input>> windowState) throws Exception {
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
        if (StringUtils.isNotEmpty(rule.getMatchingRule().getExpectedValue())) {
            return rule.getMatchingRule().getExpectedValue()
                    .equals(rule.getMappingRecord(input).get(rule.getMatchingRule().getFieldName()).toString());
        }
        return rule.getMappingRecord(input).get(rule.getMatchingRule().getFieldName())
                .equals(rule.getMappingRecord(input).get(rule.getMatchingRule().getExpectedFieldName()));
    }

    /**
     * Evaluates aggregate rule by comparing provided value with rules' limit based on operator.
     *
     * @param rule        aggregation rule to evaluate
     * @param input       inputInWindow data
     * @param windowState inputInWindow data group by time window
     * @return true if matched, otherwise false
     * @throws Exception if exception throws
     */
    private static boolean evaluateAggregatingRule(Rule rule, Input input,
                                                   MapState<Long, Set<Input>> windowState) throws Exception {
        Long windowStartTime = input.getCreatedTime() - TimeUnit.MINUTES.toMillis(rule.getWindowMinutes());

        // Calculate the aggregated value
        SimpleAccumulator<BigDecimal> aggregator = getAggregator(rule.getAggregatingRule().getAggregator());
        for (Long stateCreatedTime : windowState.keys()) {
            if (isStateValueInWindow(stateCreatedTime, windowStartTime, input.getCreatedTime())) {
                Set<Input> inputsInWindow = windowState.get(stateCreatedTime);
                for (Input inputInWindow : inputsInWindow) {
                    BigDecimal aggregatedValue = getBigDecimalByFieldName(
                            rule.getMappingRecord(inputInWindow), rule.getAggregatingRule().getAggregateFieldName());
                    aggregator.add(aggregatedValue);
                }
            }
        }
        BigDecimal actualAggregatedValue = aggregator.getLocalValue();
        rule.getAggregatingRule().setActualAggregatedValue(actualAggregatedValue);

        // compare the expected value with the actual one based on operator
        return rule.getArithmeticOperator().compare(actualAggregatedValue, rule.getAggregatingRule().getExpectedLimitValue());
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
