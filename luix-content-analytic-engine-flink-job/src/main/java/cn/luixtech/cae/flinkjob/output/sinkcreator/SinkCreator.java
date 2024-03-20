package cn.luixtech.cae.flinkjob.output.sinkcreator;

import cn.luixtech.cae.flinkjob.core.Arguments;
import com.luixtech.utilities.serviceloader.ServiceLoader;
import com.luixtech.utilities.serviceloader.annotation.Spi;
import com.luixtech.utilities.serviceloader.annotation.SpiScope;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.Optional;

@Spi(scope = SpiScope.SINGLETON)
public interface SinkCreator {
    DataStreamSink<String> create(DataStream<String> stream, Arguments arguments);

    static SinkCreator getInstance(String name) {
        return Optional.ofNullable(ServiceLoader.forClass(SinkCreator.class).load(name))
                .orElseThrow(() -> new IllegalArgumentException("Data stream sink creator [" + name + "] does NOT exist"));
    }
}
