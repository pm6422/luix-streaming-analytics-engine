package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.common.dto.Rule;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

import java.util.HashSet;
import java.util.Set;

public class ProcessingUtils {

    public static void handleRuleBroadcast(Rule rule, BroadcastState<Integer, Rule> broadcastState) throws Exception {
        switch (rule.getRuleState()) {
            case ACTIVE:
            case PAUSE:
                broadcastState.put(rule.getRuleId(), rule);
                break;
            case DELETE:
                broadcastState.remove(rule.getRuleId());
                break;
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
