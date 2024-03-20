package com.luixtech.frauddetection.flinkjob.output;

import com.luixtech.frauddetection.flinkjob.core.Arguments;
import com.luixtech.frauddetection.flinkjob.output.sinkcreator.SinkCreator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class LatencySink {

    public static DataStreamSink<String> addLatencySink(Arguments arguments, DataStream<String> stream) {
        return SinkCreator
                .getInstance("latency-" + arguments.messageChannel)
                .create(stream, arguments);
    }
}
