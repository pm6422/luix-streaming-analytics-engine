package com.luixtech.frauddetection.flinkjob.utils;

import com.luixtech.frauddetection.flinkjob.input.param.Parameters;

import java.util.Properties;

import static com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions.*;

public class KafkaPropertyUtils {


    public static Properties initProducerProperties(Parameters params) {
        return initProperties(params);
    }

    private static Properties initProperties(Parameters parameters) {
        Properties kafkaProps = new Properties();
        String kafkaHost = parameters.getValue(KAFKA_HOST);
        int kafkaPort = parameters.getValue(KAFKA_PORT);
        String servers = String.format("%s:%s", kafkaHost, kafkaPort);
        kafkaProps.setProperty("bootstrap.servers", servers);
        return kafkaProps;
    }

    public static Properties initConsumerProperties(Parameters parameters) {
        Properties kafkaProps = initProperties(parameters);
        String offset = parameters.getValue(KAFKA_OFFSET);
        kafkaProps.setProperty("auto.offset.reset", offset);
        return kafkaProps;
    }
}
