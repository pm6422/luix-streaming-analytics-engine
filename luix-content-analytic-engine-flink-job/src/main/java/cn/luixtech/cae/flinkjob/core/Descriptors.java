package cn.luixtech.cae.flinkjob.core;

import cn.luixtech.cae.common.rule.RuleCommand;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class Descriptors {
    public static final MapStateDescriptor<String, RuleCommand> RULES_COMMAND_DESCRIPTOR   = new MapStateDescriptor<>("rules", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(RuleCommand.class));
    public static final OutputTag<String>                       RULE_EVALUATION_RESULT_TAG = new OutputTag<>("rule-evaluation-result-sink") {
    };
    public static final OutputTag<Long>                         HANDLING_LATENCY_SINK_TAG  = new OutputTag<>("handling-latency-sink") {
    };
}