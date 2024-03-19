package com.luixtech.frauddetection.flinkjob.input;

import com.luixtech.frauddetection.common.input.Input;
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
public class InputSource {

    public static DataStreamSource<String> initInputSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = SourceCreator
                .getInstance("input-" + arguments.messageChannel)
                .create(env, arguments);
        dataStreamSource.setParallelism(arguments.sourceParallelism);
        return dataStreamSource;
    }

    public static DataStream<Input> stringsStreamToInput(Arguments arguments, DataStream<String> inputStrings) {
        return inputStrings
                .flatMap(new JsonDeserializer<>(Input.class))
                .name(arguments.messageChannel)
                .returns(Input.class)
                .flatMap(new TimeStamper<>())
                .returns(Input.class)
                .assignTimestampsAndWatermarks(new SimpleBoundedOutOfOrdernessTimestampExtractor<>(arguments.outOfOrderdness));
    }
}
