package com.luixtech.frauddetection.flinkjob.input;

import com.luixtech.frauddetection.flinkjob.dynamicrules.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

public abstract class SourceUtils {
    public static KafkaSource<String> getKafkaSource(InputConfig inputConfig, String topic) {
        Properties kafkaProps = KafkaUtils.initConsumerProperties(inputConfig);
        return KafkaSource.<String>builder()
                .setProperties(kafkaProps)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
