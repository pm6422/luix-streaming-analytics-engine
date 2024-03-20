package cn.luixtech.cae.flinkjob.output.sinkcreator.impl;

import cn.luixtech.cae.flinkjob.core.Arguments;
import cn.luixtech.cae.flinkjob.output.sinkcreator.SinkCreator;
import cn.luixtech.cae.flinkjob.utils.KafkaUtils;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.Properties;

@SpiName("output-" + Arguments.CHANNEL_KAFKA)
public class OutputSinkKafkaCreator implements SinkCreator {
    @Override
    public DataStreamSink<String> create(DataStream<String> stream, Arguments arguments) {
        Properties kafkaProps = KafkaUtils.initProducerProperties(arguments);
        KafkaSink<String> kafkaSink =
                KafkaSink.<String>builder()
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(arguments.outputTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
        return stream.sinkTo(kafkaSink);
    }
}
