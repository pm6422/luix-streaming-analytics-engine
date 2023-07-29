package com.luixtech.frauddetection.flinkjob.input.source;

import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.flinkjob.core.function.TimeStamper;
import com.luixtech.frauddetection.flinkjob.generator.JsonGeneratorWrapper;
import com.luixtech.frauddetection.flinkjob.generator.TransactionsGenerator;
import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.serializer.JsonDeserializer;
import com.luixtech.frauddetection.flinkjob.utils.SimpleBoundedOutOfOrdernessTimestampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.luixtech.frauddetection.flinkjob.input.SourceUtils.getKafkaSource;

@Slf4j
public class TransactionsSource {

    public static DataStreamSource<String> initTransactionsSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource;

        if (arguments.messageChannel == "kafka") {
            // Specify the topic from which the transactions are read
            KafkaSource<String> kafkaSource = getKafkaSource(arguments, arguments.transactionTopic);

            // NOTE: Idiomatically, watermarks should be assigned here, but this done later
            // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
            // TODO: refactor when FLIP-238 is added
            dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "transaction " + arguments.messageChannel);
            log.info("Created kafka based transactions source");
        } else {
            // Local transaction generator mode
            // todo: need to remove local generator
            int transactionsPerSecond = arguments.recordsPerSecond;
            JsonGeneratorWrapper<Transaction> generatorSource = new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
            dataStreamSource = env.addSource(generatorSource);
            log.info("Created local generator based transactions source");
        }
        dataStreamSource.setParallelism(arguments.sourceParallelism);
        return dataStreamSource;
    }

    public static DataStream<Transaction> stringsStreamToTransactions(Arguments arguments, DataStream<String> transactionStrings) {
        return transactionStrings
                .flatMap(new JsonDeserializer<>(Transaction.class))
                .name(arguments.messageChannel)
                .returns(Transaction.class)
                .flatMap(new TimeStamper<>())
                .returns(Transaction.class)
                .assignTimestampsAndWatermarks(new SimpleBoundedOutOfOrdernessTimestampExtractor<>(arguments.outOfOrderdness));
    }
}
