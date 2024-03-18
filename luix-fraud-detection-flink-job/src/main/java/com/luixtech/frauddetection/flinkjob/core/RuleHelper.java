package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.rule.Rule;
import com.luixtech.frauddetection.common.rule.RuleCommand;
import com.luixtech.frauddetection.common.rule.RuleEvaluationResult;
import com.luixtech.frauddetection.common.rule.RuleType;
import com.luixtech.frauddetection.common.transaction.Transaction;
import com.luixtech.frauddetection.flinkjob.core.accumulator.*;
import com.luixtech.frauddetection.flinkjob.utils.FieldsExtractor;
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

/* Collection of helper methods for Rules. */
@Slf4j
public class RuleHelper {
    public static void handleRule(BroadcastState<String, RuleCommand> broadcastState, RuleCommand ruleCommand) throws Exception {
        switch (ruleCommand.getCommand()) {
            case ADD:
                // merge rule
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

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public static RuleEvaluationResult evaluate(Rule rule, Transaction matchingRecord,
                                                MapState<Long, Set<Transaction>> windowState) throws Exception {
        return RuleType.MATCHING == rule.determineType()
                ? evaluateMatchingRule(rule, matchingRecord)
                : evaluateAggregatingRule(rule, matchingRecord, windowState);
    }

    private static RuleEvaluationResult evaluateMatchingRule(Rule rule, Transaction matchingRecord) throws IllegalAccessException, NoSuchFieldException {
        RuleEvaluationResult result = new RuleEvaluationResult();
        result.setMatched(false);
        result.setAggregateResult(BigDecimal.ZERO);

        if (StringUtils.isNotEmpty(rule.getExpectedValue())) {
            result.setMatched(rule.getExpectedValue().equals(FieldsExtractor.getFieldValAsString(matchingRecord, rule.getFieldName())));
        } else {
            result.setMatched(FieldsExtractor.isFieldValSame(matchingRecord, rule.getFieldName(), rule.getExpectedFieldName()));
        }
        return result;
    }

    private static RuleEvaluationResult evaluateAggregatingRule(Rule rule, Transaction matchingRecord,
                                                                MapState<Long, Set<Transaction>> windowState) throws Exception {
        RuleEvaluationResult result = new RuleEvaluationResult();
        result.setMatched(false);
        result.setAggregateResult(BigDecimal.ZERO);

        Long windowStartTime = matchingRecord.getCreatedTime() - TimeUnit.MINUTES.toMillis(rule.getWindowMinutes());

        // Calculate the aggregate value
        SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
        for (Long stateCreatedTime : windowState.keys()) {
            if (isStateValueInWindow(stateCreatedTime, windowStartTime, matchingRecord.getCreatedTime())) {
                Set<Transaction> transactionsInWindow = windowState.get(stateCreatedTime);
                for (Transaction t : transactionsInWindow) {
                    BigDecimal aggregatedValue = FieldsExtractor.getBigDecimalByName(t, rule.getAggregateFieldName());
                    aggregator.add(aggregatedValue);
                }
            }
        }
        BigDecimal comparisonValue = aggregator.getLocalValue();
        switch (rule.getOperator()) {
            case EQUAL:
                result.setMatched(comparisonValue.compareTo(rule.getLimit()) == 0);
                break;
            case NOT_EQUAL:
                result.setMatched(comparisonValue.compareTo(rule.getLimit()) != 0);
                break;
            case GREATER:
                result.setMatched(comparisonValue.compareTo(rule.getLimit()) > 0);
                break;
            case LESS:
                result.setMatched(comparisonValue.compareTo(rule.getLimit()) < 0);
                break;
            case GREATER_EQUAL:
                result.setMatched(comparisonValue.compareTo(rule.getLimit()) >= 0);
                break;
            case LESS_EQUAL:
                result.setMatched(comparisonValue.compareTo(rule.getLimit()) <= 0);
                break;
            default:
                throw new RuntimeException("Unknown operator: " + rule.getOperator());
        }

        return result;
    }

    private static boolean isStateValueInWindow(Long stateCreatedTime, Long windowStartTime, long currentEventTime) {
        return stateCreatedTime >= windowStartTime && stateCreatedTime <= currentEventTime;
    }
}
