package com.luixtech.frauddetection.flinkjob.output.sinkcreator.impl;

import com.luixtech.frauddetection.flinkjob.core.Arguments;
import com.luixtech.frauddetection.flinkjob.output.sinkcreator.SinkCreator;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

@SpiName("alert-" + Arguments.CHANNEL_SOCKET)
public class LatencySinkSocketCreator implements SinkCreator {
    @Override
    public DataStreamSink<String> create(DataStream<String> stream, Arguments arguments) {
        return stream.addSink(new PrintSinkFunction<>(true));
    }
}
