package cn.luixtech.cae.flinkjob.output;

import cn.luixtech.cae.common.output.Output;
import cn.luixtech.cae.flinkjob.core.Arguments;
import cn.luixtech.cae.flinkjob.output.sinkcreator.SinkCreator;
import cn.luixtech.cae.flinkjob.serializer.JsonSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class OutputSink {

    public static DataStream<String> outputStreamToString(DataStream<Output> outputs) {
        return outputs.flatMap(new JsonSerializer<>(Output.class)).name("Outputs serialization");
    }

    public static DataStreamSink<String> addOutputSink(Arguments arguments, DataStream<String> stream) {
        return SinkCreator
                .getInstance("output-" + arguments.messageChannel)
                .create(stream, arguments);
    }
}
