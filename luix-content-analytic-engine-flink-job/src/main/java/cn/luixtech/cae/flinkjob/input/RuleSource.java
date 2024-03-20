package cn.luixtech.cae.flinkjob.input;

import cn.luixtech.cae.common.rule.RuleCommand;
import cn.luixtech.cae.flinkjob.core.Arguments;
import cn.luixtech.cae.flinkjob.core.TimeStamper;
import cn.luixtech.cae.flinkjob.input.sourcecreator.SourceCreator;
import cn.luixtech.cae.flinkjob.serializer.JsonDeserializer;
import cn.luixtech.cae.flinkjob.utils.SimpleBoundedOutOfOrdernessTimestampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


@Slf4j
public class RuleSource {

    public static DataStreamSource<String> initRulesSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = SourceCreator
                .getInstance("rule-" + arguments.messageChannel)
                .create(env, arguments);
        dataStreamSource.setParallelism(1);
        return dataStreamSource;
    }

    public static DataStream<RuleCommand> stringsStreamToRules(Arguments arguments, DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new JsonDeserializer<>(RuleCommand.class))
//                .name(getRuleSourceType(parameters).getName())
                .setParallelism(1)
                .returns(RuleCommand.class)
                .flatMap(new TimeStamper<>())
                .returns(RuleCommand.class)
                .assignTimestampsAndWatermarks(new SimpleBoundedOutOfOrdernessTimestampExtractor<>(arguments.outOfOrderdness));
    }
}
