package com.luixtech.frauddetection.flinkjob.output.sinkcreator.impl;

import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.output.sinkcreator.SinkCreator;
import com.luixtech.frauddetection.flinkjob.utils.KafkaUtils;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.Properties;

@SpiName("latency-" + Arguments.CHANNEL_KAFKA)
public class LatencySinkKafkaCreator implements SinkCreator {
    @Override
    public DataStreamSink<String> create(DataStream<String> stream, Arguments arguments) {
        Properties kafkaProps = KafkaUtils.initProducerProperties(arguments);
        KafkaSink<String> kafkaSink =
                KafkaSink.<String>builder()
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(arguments.latencyTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliverGuarantee(DeliveryGuarantee.NONE)
                        .build();
        return stream.sinkTo(kafkaSink);
    }
}
