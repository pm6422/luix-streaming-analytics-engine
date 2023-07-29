package com.luixtech.frauddetection.flinkjob.input.source.creator.impl;

import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.flinkjob.generator.JsonGeneratorWrapper;
import com.luixtech.frauddetection.flinkjob.generator.TransactionsGenerator;
import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.input.source.creator.RuleSourceCreator;
import com.luixtech.utilities.serviceloader.annotation.SpiName;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SpiName("transaction-" + Arguments.CHANNEL_SOCKET)
public class TransactionSocketSourceCreator implements RuleSourceCreator {
    @Override
    public DataStreamSource<String> create(StreamExecutionEnvironment env, Arguments arguments) {
        int transactionsPerSecond = arguments.recordsPerSecond;
        JsonGeneratorWrapper<Transaction> generatorSource = new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
        return env.addSource(generatorSource);
    }
}
