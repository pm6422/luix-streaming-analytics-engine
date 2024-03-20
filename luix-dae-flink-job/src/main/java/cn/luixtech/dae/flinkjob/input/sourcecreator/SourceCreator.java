package cn.luixtech.dae.flinkjob.input.sourcecreator;

import cn.luixtech.dae.flinkjob.core.Arguments;
import com.luixtech.utilities.serviceloader.ServiceLoader;
import com.luixtech.utilities.serviceloader.annotation.Spi;
import com.luixtech.utilities.serviceloader.annotation.SpiScope;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

@Spi(scope = SpiScope.SINGLETON)
public interface SourceCreator {
    DataStreamSource<String> create(StreamExecutionEnvironment env, Arguments arguments);

    static SourceCreator getInstance(String name) {
        return Optional.ofNullable(ServiceLoader.forClass(SourceCreator.class).load(name))
                .orElseThrow(() -> new IllegalArgumentException("Data stream source creator [" + name + "] does NOT exist"));
    }
}
