package cn.luixtech.cae.flinkjob.input;

import cn.luixtech.cae.common.input.Input;
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
public class InputSource {

    public static DataStreamSource<String> initInputSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = SourceCreator
                .getInstance("input-" + arguments.messageChannel)
                .create(env, arguments);
        dataStreamSource.setParallelism(arguments.inputSourceParallelism);
        return dataStreamSource;
    }

    /**
     * Convert input string stream to structured object stream
     *
     * @param arguments         application arguments
     * @param inputStringStream input string stream
     * @return structured object stream
     */
    public static DataStream<Input> stringStreamToInput(Arguments arguments, DataStream<String> inputStringStream) {
        return inputStringStream
                .flatMap(new JsonDeserializer<>(Input.class))
                .name(arguments.messageChannel)
                .returns(Input.class)
                // set ingestion time
                .flatMap(new TimeStamper<>())
                .returns(Input.class)
                .assignTimestampsAndWatermarks(new SimpleBoundedOutOfOrdernessTimestampExtractor<>(arguments.outOfOrderdness));
    }
}
