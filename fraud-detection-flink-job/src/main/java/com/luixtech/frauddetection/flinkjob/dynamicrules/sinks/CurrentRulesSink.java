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

package com.luixtech.frauddetection.flinkjob.dynamicrules.sinks;

import com.luixtech.frauddetection.flinkjob.dynamicrules.KafkaUtils;
import com.luixtech.frauddetection.flinkjob.dynamicrules.Rule;
import com.luixtech.frauddetection.flinkjob.input.ParamHolder;
import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.serializer.JsonSerializer;
import lombok.Getter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.io.IOException;
import java.util.Properties;

public class CurrentRulesSink {

    public static DataStreamSink<String> addRulesSink(ParamHolder paramHolder, DataStream<String> stream)
            throws IOException {

        String sinkType = paramHolder.getValue(Parameters.RULES_EXPORT_SINK);
        CurrentRulesSink.Type currentRulesSinkType =
                CurrentRulesSink.Type.valueOf(sinkType.toUpperCase());
        DataStreamSink<String> dataStreamSink;

        switch (currentRulesSinkType) {
            case KAFKA:
                Properties kafkaProps = KafkaUtils.initProducerProperties(paramHolder);
                String rulesExportTopic = paramHolder.getValue(Parameters.RULES_EXPORT_TOPIC);

                KafkaSink<String> kafkaSink =
                        KafkaSink.<String>builder()
                                .setKafkaProducerConfig(kafkaProps)
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(rulesExportTopic)
                                                .setValueSerializationSchema(new SimpleStringSchema())
                                                .build())
                                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                .build();
                dataStreamSink = stream.sinkTo(kafkaSink);
                break;
            case STDOUT:
                dataStreamSink = stream.addSink(new PrintSinkFunction<>(true));
                break;
            default:
                throw new IllegalArgumentException(
                        "Source \"" + currentRulesSinkType + "\" unknown. Known values are:" + Type.values());
        }
        return dataStreamSink;
    }

    public static DataStream<String> rulesStreamToJson(DataStream<Rule> alerts) {
        return alerts.flatMap(new JsonSerializer<>(Rule.class)).name("Rules Deserialization");
    }

    @Getter
    public enum Type {
        KAFKA("Current Rules Sink (Kafka)"),
        PUBSUB("Current Rules Sink (Pub/Sub)"),
        STDOUT("Current Rules Sink (Std. Out)");

        private final String name;

        Type(String name) {
            this.name = name;
        }

    }
}
