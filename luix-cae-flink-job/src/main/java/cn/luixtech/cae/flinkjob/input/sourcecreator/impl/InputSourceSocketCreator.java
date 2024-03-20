package cn.luixtech.cae.flinkjob.input.sourcecreator.impl;

import cn.luixtech.cae.common.input.Input;
import cn.luixtech.cae.flinkjob.generator.JsonGeneratorWrapper;
import cn.luixtech.cae.flinkjob.generator.InputGenerator;
import cn.luixtech.cae.flinkjob.core.Arguments;
import cn.luixtech.cae.flinkjob.input.sourcecreator.SourceCreator;
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
