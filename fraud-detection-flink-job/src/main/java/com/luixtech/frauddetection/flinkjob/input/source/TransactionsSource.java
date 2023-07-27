package com.luixtech.frauddetection.flinkjob.input.source;

import com.luixtech.frauddetection.flinkjob.dynamicrules.functions.TimeStamper;
import com.luixtech.frauddetection.flinkjob.generator.JsonGeneratorWrapper;
import com.luixtech.frauddetection.flinkjob.generator.TransactionsGenerator;
import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.input.ParameterDefinitions;
import com.luixtech.frauddetection.flinkjob.serializer.JsonDeserializer;
import com.luixtech.frauddetection.common.dto.Transaction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.luixtech.frauddetection.flinkjob.input.ParameterDefinitions.TRANSACTIONS_SOURCE;
import static com.luixtech.frauddetection.flinkjob.input.SourceUtils.getKafkaSource;

@Slf4j
public class TransactionsSource {

    public static TransactionsSource.Type getTransactionsSourceType(Parameters parameters) {
        String transactionsSource = parameters.getValue(TRANSACTIONS_SOURCE);
        return TransactionsSource.Type.valueOf(transactionsSource.toUpperCase());
    }

    public static DataStreamSource<String> initTransactionsSource(Parameters parameters, StreamExecutionEnvironment env) {
        TransactionsSource.Type transactionsSourceType = getTransactionsSourceType(parameters);
        DataStreamSource<String> dataStreamSource;

        if (transactionsSourceType == Type.KAFKA) {
            // Specify the topic from which the transactions are read
            String transactionsTopic = parameters.getValue(ParameterDefinitions.DATA_TOPIC);
            KafkaSource<String> kafkaSource = getKafkaSource(parameters, transactionsTopic);

            // NOTE: Idiomatically, watermarks should be assigned here, but this done later
            // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
            // TODO: refactor when FLIP-238 is added
            dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), transactionsSourceType.getName());
            log.info("Created kafka based transactions source");
        } else {
            // Local generator mode
            // todo: need to remove local generator
            int transactionsPerSecond = parameters.getValue(ParameterDefinitions.RECORDS_PER_SECOND);
            JsonGeneratorWrapper<Transaction> generatorSource = new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
            dataStreamSource = env.addSource(generatorSource);
            log.info("Created local generator based transactions source");
        }
        return dataStreamSource;
    }

    public static DataStream<Transaction> stringsStreamToTransactions(DataStream<String> transactionStrings) {
        return transactionStrings
                .flatMap(new JsonDeserializer<>(Transaction.class))
                .name("Transactions Deserialization")
                .returns(Transaction.class)
                .flatMap(new TimeStamper<>())
                .returns(Transaction.class);
    }

    @Getter
    public enum Type {
        GENERATOR("Transactions Source (generated locally)"),
        KAFKA("Transactions Source (Kafka)");

        private final String name;

        Type(String name) {
            this.name = name;
        }

    }
}
