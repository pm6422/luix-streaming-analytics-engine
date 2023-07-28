package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.flinkjob.domain.Rule;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class Descriptors {
    public static final MapStateDescriptor<Integer, Rule> RULES_DESCRIPTOR       = new MapStateDescriptor<>("rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));
    public static final OutputTag<String>                 DEMO_SINK_TAG          = new OutputTag<String>("demo-sink") {
    };
    public static final OutputTag<Long>                   LATENCY_SINK_TAG       = new OutputTag<Long>("latency-sink") {
    };
    public static final OutputTag<Rule>                   CURRENT_RULES_SINK_TAG = new OutputTag<Rule>("current-rules-sink") {
    };
}