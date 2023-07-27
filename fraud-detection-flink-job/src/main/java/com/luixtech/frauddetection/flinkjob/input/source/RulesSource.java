package com.luixtech.frauddetection.flinkjob.input.source;

import com.luixtech.frauddetection.flinkjob.domain.Rule;
import com.luixtech.frauddetection.flinkjob.input.ParameterDefinitions;
import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.serializer.RuleDeserializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.luixtech.frauddetection.flinkjob.input.ParameterDefinitions.RULES_SOURCE;
import static com.luixtech.frauddetection.flinkjob.input.SourceUtils.getKafkaSource;

@Slf4j
public class RulesSource {
    public static RulesSource.Type getRuleSourceType(Parameters parameters) {
        String rulesSource = parameters.getValue(RULES_SOURCE);
        return RulesSource.Type.valueOf(rulesSource.toUpperCase());
    }

    public static DataStreamSource<String> initRulesSource(Parameters parameters, StreamExecutionEnvironment env) {
        RulesSource.Type rulesSourceType = getRuleSourceType(parameters);
        DataStreamSource<String> dataStreamSource;

        switch (rulesSourceType) {
            case KAFKA:
                // Specify the topic from which the rules are read
                String rulesTopic = parameters.getValue(ParameterDefinitions.RULES_TOPIC);
                KafkaSource<String> kafkaSource = getKafkaSource(parameters, rulesTopic);

                // NOTE: Idiomatically, watermarks should be assigned here, but this done later
                // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
                // TODO: refactor when FLIP-238 is added
                dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), rulesSourceType.getName());
                log.info("Created kafka based rules source");
                break;
            case SOCKET:
                log.info("Created local socket based rules source");
                SocketTextStreamFunction socketSourceFunction =
                        new SocketTextStreamFunction("localhost", parameters.getValue(ParameterDefinitions.SOCKET_PORT), "\n", -1);
                dataStreamSource = env.addSource(socketSourceFunction);
                break;
            default:
                throw new IllegalArgumentException(
                        "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Arrays.toString(Type.values()));
        }
        return dataStreamSource;
    }

    public static DataStream<Rule> stringsStreamToRules(Parameters parameters, DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new RuleDeserializer())
                .name("RuleDeserializer")
                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<>(Time.of(0, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(Rule element) {
                                // Prevents connected data+update stream watermark stalling.
                                return Long.MAX_VALUE;
                            }
                        })
                .name("Timestamps");
    }

    @Getter
    public enum Type {
        KAFKA("Rules Source (Kafka)"),
        PUBSUB("Rules Source (Pub/Sub)"),
        SOCKET("Rules Source (Socket)");

        private final String name;

        Type(String name) {
            this.name = name;
        }

    }
}
