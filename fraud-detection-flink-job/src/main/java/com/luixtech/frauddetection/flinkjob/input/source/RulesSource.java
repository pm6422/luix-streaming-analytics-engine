package com.luixtech.frauddetection.flinkjob.input.source;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.serializer.RuleDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

import static com.luixtech.frauddetection.flinkjob.utils.KafkaUtils.getKafkaSource;


@Slf4j
public class RulesSource {

    public static DataStreamSource<String> initRulesSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource;

        switch (arguments.messageChannel) {
            case "kafka":
                // Specify the topic from which the rules are read
                KafkaSource<String> kafkaSource = getKafkaSource(arguments, arguments.ruleTopic);

                // NOTE: Idiomatically, watermarks should be assigned here, but this done later
                // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
                // TODO: refactor when FLIP-238 is added
                dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), arguments.messageChannel);
                log.info("Created kafka based rules source");
                break;
            case "socket":
                log.info("Created local socket based rules source");
                SocketTextStreamFunction socketSourceFunction =
                        new SocketTextStreamFunction("localhost", arguments.ruleSocketPort, "\n", -1);
                dataStreamSource = env.addSource(socketSourceFunction);
                break;
            default:
                throw new IllegalArgumentException("Invalid rule source " + arguments.messageChannel);
        }
        dataStreamSource.setParallelism(1);
        return dataStreamSource;
    }

    public static DataStream<Rule> stringsStreamToRules(Arguments arguments, DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new RuleDeserializer())
//                .name(getRuleSourceType(parameters).getName())
                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<>(Time.of(0, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(Rule element) {
                                // Prevents connected data+update stream watermark stalling.
                                return Long.MAX_VALUE;
                            }
                        });
    }
}
