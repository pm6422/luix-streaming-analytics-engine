package com.luixtech.frauddetection.flinkjob.input.sourcecreator.impl;

import com.luixtech.frauddetection.common.input.Input;
import com.luixtech.frauddetection.flinkjob.generator.JsonGeneratorWrapper;
import com.luixtech.frauddetection.flinkjob.generator.InputGenerator;
import com.luixtech.frauddetection.flinkjob.core.Arguments;
import com.luixtech.frauddetection.flinkjob.input.sourcecreator.SourceCreator;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SpiName("input-" + Arguments.CHANNEL_SOCKET)
public class InputSourceSocketCreator implements SourceCreator {
    @Override
    public DataStreamSource<String> create(StreamExecutionEnvironment env, Arguments arguments) {
        int inputsPerSecond = arguments.inputsPerSecond;
        JsonGeneratorWrapper<Input> generatorSource = new JsonGeneratorWrapper<>(new InputGenerator(inputsPerSecond));
        return env.addSource(generatorSource);
    }
}
