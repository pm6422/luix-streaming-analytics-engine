package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.common.rule.ControlType;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

import java.util.*;

@Slf4j
public class ProcessingUtils {

    public static void handleRule(BroadcastState<Integer, Rule> broadcastState, Rule rule) throws Exception {
        switch (rule.getRuleState()) {
            case ACTIVE:
            case PAUSE:
                // merge rule
                broadcastState.put(rule.getRuleId(), rule);
                break;
            case DELETE:
                broadcastState.remove(rule.getRuleId());
                break;
            case CONTROL:
                handleControlCommand(broadcastState, rule.getControlType());
                break;
        }
    }

    private static void handleControlCommand(BroadcastState<Integer, Rule> rulesState, ControlType controlType) throws Exception {
        if (Objects.requireNonNull(controlType) == ControlType.DELETE_ALL_RULES) {
            Iterator<Map.Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
            while (entriesIterator.hasNext()) {
                Map.Entry<Integer, Rule> ruleEntry = entriesIterator.next();
                rulesState.remove(ruleEntry.getKey());
                log.info("Removed {}", ruleEntry.getValue());
            }
        }
    }

    public static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value) throws Exception {
        Set<V> valuesSet = mapState.get(key);
        if (valuesSet == null) {
            valuesSet = new HashSet<>();
        }
        valuesSet.add(value);
        mapState.put(key, valuesSet);
        return valuesSet;
    }
}
