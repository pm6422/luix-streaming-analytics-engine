/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.luixtech.frauddetection.flinkjob.dynamicrules;

import com.luixtech.frauddetection.flinkjob.input.Parameters;

import java.util.Properties;

import static com.luixtech.frauddetection.flinkjob.input.ParameterDefinitions.*;

public class KafkaUtils {

    public static Properties initConsumerProperties(Parameters parameters) {
        Properties kafkaProps = initProperties(parameters);
        String offset = parameters.getValue(OFFSET);
        kafkaProps.setProperty("auto.offset.reset", offset);
        return kafkaProps;
    }

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
}
