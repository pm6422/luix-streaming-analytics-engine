package cn.luixtech.dae.flinkjob.output;

import cn.luixtech.dae.common.output.Output;
import cn.luixtech.dae.flinkjob.core.Arguments;
import cn.luixtech.dae.flinkjob.output.sinkcreator.SinkCreator;
import cn.luixtech.dae.flinkjob.serializer.JsonSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class OutputSink {

    public static DataStream<String> outputStreamToString(DataStream<Output> outputStream) {
        return outputStream.flatMap(new JsonSerializer<>(Output.class)).name("Outputs serialization");
    }

    public static DataStreamSink<String> addOutputSink(Arguments arguments, DataStream<String> stringStream) {
        return SinkCreator
                .getInstance("output-" + arguments.messageChannel)
                .create(stringStream, arguments);
    }
}
