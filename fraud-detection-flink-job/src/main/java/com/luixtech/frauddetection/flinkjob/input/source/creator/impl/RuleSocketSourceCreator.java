package com.luixtech.frauddetection.flinkjob.input.source.creator.impl;

import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.input.source.creator.RuleSourceCreator;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;

@SpiName("rule-" + Arguments.CHANNEL_SOCKET)
public class RuleSocketSourceCreator implements RuleSourceCreator {
    @Override
    public DataStreamSource<String> create(StreamExecutionEnvironment env, Arguments arguments) {
        SocketTextStreamFunction socketSourceFunction =
                new SocketTextStreamFunction("localhost", arguments.ruleSocketPort, "\n", -1);
        return env.addSource(socketSourceFunction);
    }
}
