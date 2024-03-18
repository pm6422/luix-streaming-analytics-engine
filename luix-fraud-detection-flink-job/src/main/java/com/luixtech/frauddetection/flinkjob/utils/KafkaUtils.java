package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.flinkjob.core.Arguments;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

public class KafkaUtils {

    public static KafkaSource<String> createKafkaSource(Arguments arguments, String topic, String group) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(arguments.kafkaHost + ":" + arguments.kafkaPort)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static Properties initProducerProperties(Arguments arguments) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", arguments.kafkaHost + ":" + arguments.kafkaPort);
        // 2 minutes
//        properties.setProperty("transaction.timeout.ms", "120000");
        return properties;
    }
}
