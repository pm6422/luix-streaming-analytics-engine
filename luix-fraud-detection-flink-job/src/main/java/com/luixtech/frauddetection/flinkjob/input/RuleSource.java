package com.luixtech.frauddetection.flinkjob.input;

import com.luixtech.frauddetection.common.dto.RuleCommand;
import com.luixtech.frauddetection.flinkjob.core.Arguments;
import com.luixtech.frauddetection.flinkjob.core.TimeStamper;
import com.luixtech.frauddetection.flinkjob.input.sourcecreator.SourceCreator;
import com.luixtech.frauddetection.flinkjob.serializer.JsonDeserializer;
import com.luixtech.frauddetection.flinkjob.utils.SimpleBoundedOutOfOrdernessTimestampExtractor;
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
