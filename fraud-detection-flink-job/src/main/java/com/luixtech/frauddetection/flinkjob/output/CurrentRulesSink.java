package com.luixtech.frauddetection.flinkjob.output;

import com.luixtech.frauddetection.common.dto.Rule;
import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.serializer.JsonSerializer;
import com.luixtech.frauddetection.flinkjob.utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.util.Properties;

public class CurrentRulesSink {

    public static DataStreamSink<String> addRulesSink(Arguments arguments, DataStream<String> stream) {
        DataStreamSink<String> dataStreamSink;

        switch (arguments.messageChannel) {
            case "kafka":
                Properties kafkaProps = KafkaUtils.initProducerProperties(arguments);
                KafkaSink<String> kafkaSink =
                        KafkaSink.<String>builder()
                                .setKafkaProducerConfig(kafkaProps)
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(arguments.currentRuleTopic)
                                                .setValueSerializationSchema(new SimpleStringSchema())
                                                .build())
                                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                .build();
                dataStreamSink = stream.sinkTo(kafkaSink);
                break;
            case "socket":
                dataStreamSink = stream.addSink(new PrintSinkFunction<>(true));
                break;
            default:
                throw new IllegalArgumentException("Invalid current rule sink " + arguments.messageChannel);
        }
        return dataStreamSink;
    }

    public static DataStream<String> rulesStreamToJson(DataStream<Rule> alerts) {
        return alerts.flatMap(new JsonSerializer<>(Rule.class)).name("Rules Deserialization");
    }
}
