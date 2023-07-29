package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.flinkjob.core.Arguments;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

public class KafkaUtils {

    public static KafkaSource<String> createKafkaSource(Arguments arguments, String topic) {
        Properties kafkaProps = initConsumerProperties(arguments);
        return KafkaSource.<String>builder()
                .setProperties(kafkaProps)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static Properties initProducerProperties(Arguments arguments) {
        return initProperties(arguments);
    }

    public static Properties initConsumerProperties(Arguments arguments) {
        Properties kafkaProps = initProperties(arguments);
        kafkaProps.setProperty("auto.offset.reset", arguments.kafkaOffset);
        return kafkaProps;
    }

    private static Properties initProperties(Arguments arguments) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", arguments.kafkaHost + ":" + arguments.kafkaPort);
        return kafkaProps;
    }
}
