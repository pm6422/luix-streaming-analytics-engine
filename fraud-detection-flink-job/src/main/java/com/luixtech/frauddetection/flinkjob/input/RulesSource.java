package com.luixtech.frauddetection.flinkjob.input;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.flinkjob.input.sourcecreator.SourceCreator;
import com.luixtech.frauddetection.flinkjob.serializer.RuleDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;


@Slf4j
public class RulesSource {

    public static DataStreamSource<String> initRulesSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = SourceCreator
                .getInstance("rule-" + arguments.messageChannel)
                .create(env, arguments);
        dataStreamSource.setParallelism(1);
        return dataStreamSource;
    }

    public static DataStream<Rule> stringsStreamToRules(Arguments arguments, DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new RuleDeserializer())
//                .name(getRuleSourceType(parameters).getName())
                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<>(Time.of(0, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(Rule element) {
                                // Prevents connected data+update stream watermark stalling.
                                return Long.MAX_VALUE;
                            }
                        });
    }
}
