package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.flinkjob.input.Arguments;

import java.util.Properties;

public class KafkaPropertyUtils {

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
