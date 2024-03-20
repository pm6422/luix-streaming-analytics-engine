package cn.luixtech.cae.flinkjob.output.sinkcreator.impl;

import cn.luixtech.cae.flinkjob.core.Arguments;
import cn.luixtech.cae.flinkjob.output.sinkcreator.SinkCreator;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

@SpiName("output-" + Arguments.CHANNEL_SOCKET)
public class OutputSinkSocketCreator implements SinkCreator {
    @Override
    public DataStreamSink<String> create(DataStream<String> stream, Arguments arguments) {
        return stream.addSink(new PrintSinkFunction<>(true));
    }
}
