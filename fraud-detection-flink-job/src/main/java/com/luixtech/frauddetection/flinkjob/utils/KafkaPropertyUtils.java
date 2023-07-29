package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.flinkjob.input.Arguments;

import java.util.Properties;

public class KafkaPropertyUtils {


    public static Properties initProducerProperties(Arguments arguments) {
        return initProperties(arguments);
    }

    private static Properties initProperties(Arguments arguments) {
        Properties kafkaProps = new Properties();
        String kafkaHost = arguments.kafkaHost;
        int kafkaPort = arguments.kafkaPort;
        String servers = String.format("%s:%s", kafkaHost, kafkaPort);
        kafkaProps.setProperty("bootstrap.servers", servers);
        return kafkaProps;
    }

    public static Properties initConsumerProperties(Arguments arguments) {
        Properties kafkaProps = initProperties(arguments);
        String offset = arguments.kafkaOffset;
        kafkaProps.setProperty("auto.offset.reset", offset);
        return kafkaProps;
    }
}
