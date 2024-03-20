package cn.luixtech.cae.flinkjob.core;

import cn.luixtech.cae.common.input.Input;
import cn.luixtech.cae.common.rule.*;
import cn.luixtech.cae.common.rule.aggregating.Aggregator;
import cn.luixtech.cae.flinkjob.core.accumulator.*;
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
    public static void handleRuleCommand(BroadcastState<String, RuleCommand> broadcastState, RuleCommand ruleCommand) throws Exception {
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

    public static boolean evaluateRuleGroup(RuleGroup ruleGroup, Input input, MapState<Long, Set<Input>> inputWindowState) throws Exception {
        if (ruleGroup == null) {
            return false;
        }
        if (StringUtils.isNotEmpty(ruleGroup.getTenant()) && !ruleGroup.getTenant().equals(input.getTenant())) {
            // return false if existing tenant value of rule is not identical to the one of input object
            return false;
        }
        boolean result = ruleGroup.getLogicalOperator() == LogicalOperator.AND;

        // evaluate child rule groups
        if (CollectionUtils.isNotEmpty(ruleGroup.getChildren())) {
            for (RuleGroup childGroup : ruleGroup.getChildren()) {
                boolean childResult = evaluateRuleGroup(childGroup, input, inputWindowState);
                if (ruleGroup.getLogicalOperator() == LogicalOperator.AND) {
                    result = result && childResult;
                } else {
                    result = result || childResult;
                }
            }
        }

        // evaluate rules
        if (CollectionUtils.isNotEmpty(ruleGroup.getRules())) {
            for (Rule rule : ruleGroup.getRules()) {
                boolean ruleResult = evaluateRule(rule, input, inputWindowState);
                if (ruleGroup.getLogicalOperator() == LogicalOperator.AND) {
                    result = result && ruleResult;
                } else {
                    result = result || ruleResult;
                }
            }
        }
        return result;
    }

    public static boolean evaluateRule(Rule rule, Input input, MapState<Long, Set<Input>> inputWindowState) throws Exception {
        return RuleType.MATCHING == rule.determineType()
                ? evaluateMatchingRule(rule, input)
                : evaluateAggregatingRule(rule, input, inputWindowState);
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
            // expected value matching
            return rule.getMatchingRule().getExpectedValue()
                    .equals(input.getRecRefFieldValue(rule, rule.getMatchingRule().getFieldName()));
        }
        // field value matching
        return input.getRecRefFieldValue(rule, rule.getMatchingRule().getFieldName())
                .equals(input.getRecTargetFieldValue(rule, rule.getMatchingRule().getExpectedFieldName()));
    }

    /**
     * Evaluates aggregate rule by comparing provided value with rules' limit based on operator.
     *
     * @param rule             aggregation rule to evaluate
     * @param input            inputInWindow data
     * @param inputWindowState inputInWindow data group by time window
     * @return true if matched, otherwise false
     * @throws Exception if exception throws
     */
    private static boolean evaluateAggregatingRule(Rule rule, Input input, MapState<Long, Set<Input>> inputWindowState) throws Exception {
        Long windowStartTime = input.getCreatedTime() - TimeUnit.MINUTES.toMillis(rule.getWindowMinutes());

        // Calculate the aggregated value
        SimpleAccumulator<BigDecimal> aggregator = getAggregator(rule.getAggregatingRule().getAggregator());
        for (Long inputStateCreatedTime : inputWindowState.keys()) {
            if (isStateInWindow(inputStateCreatedTime, windowStartTime, input.getCreatedTime())) {
                Set<Input> inputsInWindow = inputWindowState.get(inputStateCreatedTime);
                for (Input inputInWindow : inputsInWindow) {
                    BigDecimal aggregatedValue = getBigDecimalByFieldName(
                            inputInWindow.getRecRefMap(rule), rule.getAggregatingRule().getAggregateFieldName());
                    aggregator.add(aggregatedValue);
                }
            }
        }
        BigDecimal actualAggregatedValue = aggregator.getLocalValue();
        rule.getAggregatingRule().setActualAggregatedValue(actualAggregatedValue);

        // compare the expected value with the actual one based on operator
        return rule.getArithmeticOperator().compare(actualAggregatedValue, rule.getAggregatingRule().getExpectedLimitValue());
    }

    private static boolean isStateInWindow(Long inputStateCreatedTime, Long windowStartTime, long currentInputCreatedTime) {
        return inputStateCreatedTime >= windowStartTime && inputStateCreatedTime <= currentInputCreatedTime;
    }

    private static BigDecimal getBigDecimalByFieldName(Map<String, Object> record, String fieldName) {
        if (StringUtils.isEmpty(fieldName) || !record.containsKey(fieldName)) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(record.get(fieldName).toString());
    }
}
