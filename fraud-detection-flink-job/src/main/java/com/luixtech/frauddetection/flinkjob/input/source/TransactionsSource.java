package com.luixtech.frauddetection.flinkjob.input.source;

import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.flinkjob.core.MessageChannel;
import com.luixtech.frauddetection.flinkjob.core.function.TimeStamper;
import com.luixtech.frauddetection.flinkjob.generator.JsonGeneratorWrapper;
import com.luixtech.frauddetection.flinkjob.generator.TransactionsGenerator;
import com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions;
import com.luixtech.frauddetection.flinkjob.input.param.Parameters;
import com.luixtech.frauddetection.flinkjob.serializer.JsonDeserializer;
import com.luixtech.frauddetection.flinkjob.utils.SimpleBoundedOutOfOrdernessTimestampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.luixtech.frauddetection.flinkjob.core.MessageChannel.KAFKA;
import static com.luixtech.frauddetection.flinkjob.input.SourceUtils.getKafkaSource;
import static com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions.*;
import static com.luixtech.utilities.lang.EnumValueHoldable.getEnumByValue;

@Slf4j
public class TransactionsSource {

    public static DataStreamSource<String> initTransactionsSource(Parameters parameters, StreamExecutionEnvironment env) {
        MessageChannel messageChannel = getEnumByValue(MessageChannel.class, parameters.getValue(MESSAGE_CHANNEL));
        DataStreamSource<String> dataStreamSource;

        if (messageChannel == KAFKA) {
            // Specify the topic from which the transactions are read
            String transactionsTopic = parameters.getValue(ParameterDefinitions.DATA_TOPIC);
            KafkaSource<String> kafkaSource = getKafkaSource(parameters, transactionsTopic);

            // NOTE: Idiomatically, watermarks should be assigned here, but this done later
            // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
            // TODO: refactor when FLIP-238 is added
            dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), messageChannel.getValue());
            log.info("Created kafka based transactions source");
        } else {
            // Local transaction generator mode
            // todo: need to remove local generator
            int transactionsPerSecond = parameters.getValue(ParameterDefinitions.RECORDS_PER_SECOND);
            JsonGeneratorWrapper<Transaction> generatorSource = new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
            dataStreamSource = env.addSource(generatorSource);
            log.info("Created local generator based transactions source");
        }
        dataStreamSource.setParallelism(parameters.getValue(SOURCE_PARALLELISM));
        return dataStreamSource;
    }

    public static DataStream<Transaction> stringsStreamToTransactions(Parameters parameters, DataStream<String> transactionStrings) {
        MessageChannel messageChannel = getEnumByValue(MessageChannel.class, parameters.getValue(MESSAGE_CHANNEL));
        return transactionStrings
                .flatMap(new JsonDeserializer<>(Transaction.class))
                .name(messageChannel.getValue())
                .returns(Transaction.class)
                .flatMap(new TimeStamper<>())
                .returns(Transaction.class)
                .assignTimestampsAndWatermarks(new SimpleBoundedOutOfOrdernessTimestampExtractor<>(parameters.getValue(OUT_OF_ORDERNESS)));
    }
}
