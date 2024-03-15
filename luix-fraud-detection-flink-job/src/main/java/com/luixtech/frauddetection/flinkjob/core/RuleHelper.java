package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.rule.RuleControl;
import com.luixtech.frauddetection.flinkjob.core.accumulator.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

/* Collection of helper methods for Rules. */
@Slf4j
public class RuleHelper {
    public static void handleRule(BroadcastState<Integer, Rule> broadcastState, Rule rule) throws Exception {
        switch (rule.getRuleState()) {
            case ACTIVE:
                // merge rule
                broadcastState.put(rule.getRuleId(), rule);
                break;
            case DELETE:
            case PAUSE:
                broadcastState.remove(rule.getRuleId());
                break;
            case CONTROL:
                if (RuleControl.DELETE_ALL_RULES == rule.getRuleControl()) {
                    Iterator<Map.Entry<Integer, Rule>> entriesIterator = broadcastState.iterator();
                    while (entriesIterator.hasNext()) {
                        Map.Entry<Integer, Rule> ruleEntry = entriesIterator.next();
                        broadcastState.remove(ruleEntry.getKey());
                        log.info("Removed {}", ruleEntry.getValue());
                    }
                }
                break;
        }
    }

    /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        switch (rule.getAggregatorFunctionType()) {
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
                        "Unsupported aggregation function type: " + rule.getAggregatorFunctionType());
        }
    }
}
