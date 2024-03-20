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

    public static DataStreamSource<String> initRuleCommandSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = SourceCreator
                .getInstance("rule-" + arguments.messageChannel)
                .create(env, arguments);
        dataStreamSource.setParallelism(1);
        return dataStreamSource;
    }

    /**
     * Convert rule command string stream to structured object stream
     *
     * @param arguments               application arguments
     * @param ruleCommandStringStream rule command string stream
     * @return structured object stream
     */
    public static DataStream<RuleCommand> stringStreamToRuleCommand(Arguments arguments, DataStream<String> ruleCommandStringStream) {
        return ruleCommandStringStream
                .flatMap(new JsonDeserializer<>(RuleCommand.class))
//                .name(getRuleSourceType(parameters).getName())
                .setParallelism(1)
                .returns(RuleCommand.class)
                // set ingestion time
                .flatMap(new TimeStamper<>())
                .returns(RuleCommand.class)
                .assignTimestampsAndWatermarks(new SimpleBoundedOutOfOrdernessTimestampExtractor<>(arguments.outOfOrderdness));
    }
}
