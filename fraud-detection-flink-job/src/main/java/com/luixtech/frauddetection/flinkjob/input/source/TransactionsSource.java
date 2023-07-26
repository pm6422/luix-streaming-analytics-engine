package com.luixtech.frauddetection.flinkjob.input.source;

import com.luixtech.frauddetection.flinkjob.dynamicrules.KafkaUtils;
import com.luixtech.frauddetection.flinkjob.dynamicrules.functions.JsonDeserializer;
import com.luixtech.frauddetection.flinkjob.dynamicrules.functions.JsonGeneratorWrapper;
import com.luixtech.frauddetection.flinkjob.dynamicrules.functions.TimeStamper;
import com.luixtech.frauddetection.flinkjob.input.InputConfig;
import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.transaction.generator.TransactionsGenerator;
import com.luixtech.frauddetection.flinkjob.transaction.domain.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import static com.luixtech.frauddetection.flinkjob.input.Parameters.TRANSACTIONS_SOURCE;
import static com.luixtech.frauddetection.flinkjob.input.SourceUtils.getKafkaSource;

public class TransactionsSource {

    public static TransactionsSource.Type getTransactionsSourceType(InputConfig inputConfig) {
        String transactionsSource = inputConfig.get(TRANSACTIONS_SOURCE);
        return TransactionsSource.Type.valueOf(transactionsSource.toUpperCase());
    }

    public static DataStreamSource<String> initTransactionsSource(InputConfig inputConfig, StreamExecutionEnvironment env) {
        TransactionsSource.Type transactionsSourceType = getTransactionsSourceType(inputConfig);
        DataStreamSource<String> dataStreamSource;

        if (transactionsSourceType == Type.KAFKA) {
            // Specify the topic from which the transactions are read
            String transactionsTopic = inputConfig.get(Parameters.DATA_TOPIC);
            KafkaSource<String> kafkaSource = getKafkaSource(inputConfig, transactionsTopic);

            // NOTE: Idiomatically, watermarks should be assigned here, but this done later
            // because of the mix of the new Source (Kafka) and SourceFunction-based interfaces.
            // TODO: refactor when FLIP-238 is added
            dataStreamSource =
                    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), transactionsSourceType.getName());
        } else {
            // Local generator mode
            // todo: need to remove local generator
            int transactionsPerSecond = inputConfig.get(Parameters.RECORDS_PER_SECOND);
            JsonGeneratorWrapper<Transaction> generatorSource = new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
            dataStreamSource = env.addSource(generatorSource);
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

    public enum Type {
        GENERATOR("Transactions Source (generated locally)"),
        KAFKA("Transactions Source (Kafka)");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
