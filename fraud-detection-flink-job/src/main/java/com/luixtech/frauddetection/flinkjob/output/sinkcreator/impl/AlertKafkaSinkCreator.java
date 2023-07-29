package com.luixtech.frauddetection.flinkjob.output.sinkcreator.impl;

import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.input.sourcecreator.SourceCreator;
import com.luixtech.frauddetection.flinkjob.output.sinkcreator.SinkCreator;
import com.luixtech.frauddetection.flinkjob.utils.KafkaUtils;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import static com.luixtech.frauddetection.flinkjob.utils.KafkaUtils.createKafkaSource;

@SpiName("alert-" + Arguments.CHANNEL_KAFKA)
public class AlertKafkaSinkCreator implements SinkCreator {
    @Override
    public DataStreamSink<String> create(DataStream<String> stream, Arguments arguments) {
        Properties kafkaProps = KafkaUtils.initProducerProperties(arguments);
        KafkaSink<String> kafkaSink =
                KafkaSink.<String>builder()
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(arguments.alertTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
        return stream.sinkTo(kafkaSink);
    }
}
