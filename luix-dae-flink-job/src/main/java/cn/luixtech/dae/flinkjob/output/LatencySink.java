package cn.luixtech.dae.flinkjob.output;

import cn.luixtech.dae.flinkjob.core.Arguments;
import cn.luixtech.dae.flinkjob.output.sinkcreator.SinkCreator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class LatencySink {

    public static DataStreamSink<String> addLatencySink(Arguments arguments, DataStream<String> stream) {
        return SinkCreator
                .getInstance("latency-" + arguments.messageChannel)
                .create(stream, arguments);
    }
}
