package com.luixtech.frauddetection.flinkjob.output;

import com.luixtech.frauddetection.flinkjob.domain.Rule;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class Descriptors {
    public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
            new MapStateDescriptor<>("rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

    public static final OutputTag<String> demoSinkTag         = new OutputTag<String>("demo-sink") {
    };
    public static final OutputTag<Long>   latencySinkTag      = new OutputTag<Long>("latency-sink") {
    };
    public static final OutputTag<Rule>   currentRulesSinkTag = new OutputTag<Rule>("current-rules-sink") {
    };
}