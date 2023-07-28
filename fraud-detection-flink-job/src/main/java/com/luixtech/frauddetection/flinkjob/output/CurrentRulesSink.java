package com.luixtech.frauddetection.flinkjob.output;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions;
import com.luixtech.frauddetection.flinkjob.input.param.Parameters;
import com.luixtech.frauddetection.flinkjob.serializer.JsonSerializer;
import com.luixtech.frauddetection.flinkjob.utils.KafkaPropertyUtils;
import lombok.Getter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.util.Arrays;
import java.util.Properties;

import static com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions.CURRENT_RULES_SINK;

public class CurrentRulesSink {

    public static CurrentRulesSink.Type getRuleSourceType(Parameters parameters) {
        String currentRules = parameters.getValue(CURRENT_RULES_SINK);
        return CurrentRulesSink.Type.valueOf(currentRules.toUpperCase());
    }

    public static DataStreamSink<String> addRulesSink(Parameters parameters, DataStream<String> stream) {
        CurrentRulesSink.Type currentRulesSinkType = getRuleSourceType(parameters);
        DataStreamSink<String> dataStreamSink;

        switch (currentRulesSinkType) {
            case KAFKA:
                Properties kafkaProps = KafkaPropertyUtils.initProducerProperties(parameters);
                String currentRulesTopic = parameters.getValue(ParameterDefinitions.CURRENT_RULES_TOPIC);

                KafkaSink<String> kafkaSink =
                        KafkaSink.<String>builder()
                                .setKafkaProducerConfig(kafkaProps)
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(currentRulesTopic)
                                                .setValueSerializationSchema(new SimpleStringSchema())
                                                .build())
                                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                .build();
                dataStreamSink = stream.sinkTo(kafkaSink);
                break;
            case STDOUT:
                dataStreamSink = stream.addSink(new PrintSinkFunction<>(true));
                break;
            default:
                throw new IllegalArgumentException(
                        "Source \"" + currentRulesSinkType + "\" unknown. Known values are:" + Arrays.toString(Type.values()));
        }
        return dataStreamSink;
    }

    public static DataStream<String> rulesStreamToJson(DataStream<Rule> alerts) {
        return alerts.flatMap(new JsonSerializer<>(Rule.class)).name("Rules Deserialization");
    }

    @Getter
    public enum Type {
        KAFKA("Current Rules Sink (Kafka)"),
        //        PUBSUB("Current Rules Sink (Pub/Sub)"),
        STDOUT("Current Rules Sink (Std. Out)");

        private final String name;

        Type(String name) {
            this.name = name;
        }
    }
}
