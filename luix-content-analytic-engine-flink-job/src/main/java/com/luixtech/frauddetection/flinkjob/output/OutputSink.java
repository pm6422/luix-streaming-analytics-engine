package com.luixtech.frauddetection.flinkjob.output;

import com.luixtech.frauddetection.common.output.Output;
import com.luixtech.frauddetection.flinkjob.core.Arguments;
import com.luixtech.frauddetection.flinkjob.output.sinkcreator.SinkCreator;
import com.luixtech.frauddetection.flinkjob.serializer.JsonSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class OutputSink {

    public static DataStream<String> outputStreamToJson(DataStream<Output> outputs) {
        return outputs.flatMap(new JsonSerializer<>(Output.class)).name("Outputs Serialization");
    }

    public static DataStreamSink<String> addOutputSink(Arguments arguments, DataStream<String> stream) {
        return SinkCreator
                .getInstance("output-" + arguments.messageChannel)
                .create(stream, arguments);
    }
}
