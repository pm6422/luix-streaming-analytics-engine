package com.luixtech.frauddetection.flinkjob.input.source.creator.impl;

import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.input.source.creator.RuleSourceCreator;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.luixtech.frauddetection.flinkjob.utils.KafkaUtils.createKafkaSource;

@SpiName("transaction-" + Arguments.CHANNEL_KAFKA)
public class TransactionKafkaSourceCreator implements RuleSourceCreator {
    @Override
    public DataStreamSource<String> create(StreamExecutionEnvironment env, Arguments arguments) {
        KafkaSource<String> kafkaSource = createKafkaSource(arguments, arguments.transactionTopic);

        // NOTE: Idiomatically, watermarks should be assigned here, but this done later
        // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
        // TODO: refactor when FLIP-238 is added
        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), arguments.messageChannel);
    }
}
