package com.luixtech.frauddetection.flinkjob.input.source.creator;

import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.utilities.serviceloader.ServiceLoader;
import com.luixtech.utilities.serviceloader.annotation.Spi;
import com.luixtech.utilities.serviceloader.annotation.SpiScope;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

@Spi(scope = SpiScope.SINGLETON)
public interface RuleSourceCreator {
    DataStreamSource<String> create(StreamExecutionEnvironment env, Arguments arguments);

    static RuleSourceCreator getInstance(String name) {
        return Optional.ofNullable(ServiceLoader.forClass(RuleSourceCreator.class).load(name))
                .orElseThrow(() -> new IllegalArgumentException("Data stream source creator [" + name + "] does NOT exist"));
    }
}
