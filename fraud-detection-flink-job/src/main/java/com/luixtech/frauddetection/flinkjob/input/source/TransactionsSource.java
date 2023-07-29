package com.luixtech.frauddetection.flinkjob.input.source;

import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.flinkjob.core.function.TimeStamper;
import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.input.source.creator.RuleSourceCreator;
import com.luixtech.frauddetection.flinkjob.serializer.JsonDeserializer;
import com.luixtech.frauddetection.flinkjob.utils.SimpleBoundedOutOfOrdernessTimestampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class TransactionsSource {

    public static DataStreamSource<String> initTransactionsSource(Arguments arguments, StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = RuleSourceCreator
                .getInstance("transaction-" + arguments.messageChannel)
                .create(env, arguments);
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
