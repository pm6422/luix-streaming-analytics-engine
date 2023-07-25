package com.luixtech.frauddetection.flinkjob.transaction.rule;

import com.luixtech.frauddetection.flinkjob.dynamicrules.Alert;
import com.luixtech.frauddetection.flinkjob.dynamicrules.Rule;
import com.luixtech.frauddetection.flinkjob.dynamicrules.functions.AverageAggregate;
import com.luixtech.frauddetection.flinkjob.dynamicrules.sinks.AlertsSink;
import com.luixtech.frauddetection.flinkjob.dynamicrules.sinks.CurrentRulesSink;
import com.luixtech.frauddetection.flinkjob.dynamicrules.sinks.LatencySink;
import com.luixtech.frauddetection.flinkjob.input.InputConfig;
import com.luixtech.frauddetection.flinkjob.input.source.RulesSource;
import com.luixtech.frauddetection.flinkjob.input.source.TransactionsSource;
import com.luixtech.frauddetection.flinkjob.output.Descriptors;
import com.luixtech.frauddetection.flinkjob.transaction.alert.DynamicAlertFunction;
import com.luixtech.frauddetection.flinkjob.transaction.domain.Transaction;
import com.luixtech.frauddetection.flinkjob.transaction.key.DynamicKeyFunction;
import com.luixtech.frauddetection.flinkjob.utils.SimpleBoundedOutOfOrdernessTimestampExtractor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.luixtech.frauddetection.flinkjob.input.Parameters.*;
import static com.luixtech.frauddetection.flinkjob.input.source.RulesSource.*;
import static com.luixtech.frauddetection.flinkjob.input.source.TransactionsSource.*;

@Slf4j
@AllArgsConstructor
public class RulesEvaluator {

    private final InputConfig inputConfig;

    public void run() throws Exception {
        // Configure execution environment
        StreamExecutionEnvironment env = configureStreamExecutionEnv();

        // Streams setup
        DataStream<Rule> rulesStream = getRulesStream(env);
        DataStream<Transaction> transactionsStream = getTransactionsStream(env);
        // Create a broadcast rules stream
        BroadcastStream<Rule> broadcastRulesStream = rulesStream.broadcast(Descriptors.rulesDescriptor);

        // Processing pipeline setup
        DataStream<Alert> alertsStream = transactionsStream
                .connect(broadcastRulesStream)
                .process(new DynamicKeyFunction())
                .uid("DynamicKeyFunction")
                .name("Dynamic Partitioning Function")
                .keyBy((keyed) -> keyed.getKey())
                .connect(broadcastRulesStream)
                .process(new DynamicAlertFunction())
                .uid("DynamicAlertFunction")
                .name("Dynamic Rule Evaluation Function");

        DataStream<String> allRuleEvaluations =
                ((SingleOutputStreamOperator<Alert>) alertsStream).getSideOutput(Descriptors.demoSinkTag);

        DataStream<Long> latency =
                ((SingleOutputStreamOperator<Alert>) alertsStream).getSideOutput(Descriptors.latencySinkTag);

        DataStream<Rule> currentRules =
                ((SingleOutputStreamOperator<Alert>) alertsStream).getSideOutput(Descriptors.currentRulesSinkTag);

        alertsStream.print().name("Alert STDOUT Sink");
        allRuleEvaluations.print().setParallelism(1).name("Rule Evaluation Sink");

        DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alertsStream);
        DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);

        currentRulesJson.print();

        DataStreamSink<String> alertsSink = AlertsSink.addAlertsSink(inputConfig, alertsJson);
        alertsSink.setParallelism(1).name("Alerts JSON Sink");

        DataStreamSink<String> currentRulesSink = CurrentRulesSink.addRulesSink(inputConfig, currentRulesJson);
        currentRulesSink.setParallelism(1);

        DataStream<String> latencies = latency
                        .timeWindowAll(Time.seconds(10))
                        .aggregate(new AverageAggregate())
                        .map(String::valueOf);

        DataStreamSink<String> latencySink = LatencySink.addLatencySink(inputConfig, latencies);
        latencySink.name("Latency Sink");

        env.execute("Fraud Detection Engine");
    }

    private StreamExecutionEnvironment configureStreamExecutionEnv() {
        StreamExecutionEnvironment env = getStreamExecutionEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (inputConfig.get(ENABLE_CHECKPOINTS)) {
            env.enableCheckpointing(inputConfig.get(CHECKPOINT_INTERVAL));
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(inputConfig.get(MIN_PAUSE_BETWEEN_CHECKPOINTS));
        }
        configureRestartStrategy(env);
        return env;
    }

    private StreamExecutionEnvironment getStreamExecutionEnv() {
        if (inputConfig.get(LOCAL_EXECUTION)) {
            // Create an embedded Flink execution environment with flink UI dashboard
            Configuration flinkConfig = new Configuration();
            flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        }
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private void configureRestartStrategy(StreamExecutionEnvironment env) {
        RulesSource.Type rulesSourceEnumType = getRulesSourceType(inputConfig);
        switch (rulesSourceEnumType) {
            case SOCKET:
                env.setRestartStrategy(
                        RestartStrategies.fixedDelayRestart(
                                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
                break;
            case KAFKA:
                // Default - unlimited restart strategy.
                //        env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    private DataStream<Rule> getRulesStream(StreamExecutionEnvironment env) throws IOException {
        RulesSource.Type rulesSourceType = getRulesSourceType(inputConfig);
        DataStream<String> rulesStringStream = initRulesSource(inputConfig, env)
                // todo: put below in initRulesSource method
                .name(rulesSourceType.getName())
                .setParallelism(1);
        return stringsStreamToRules(rulesStringStream);
    }

    private DataStream<Transaction> getTransactionsStream(StreamExecutionEnvironment env) {
        TransactionsSource.Type transactionsSourceType = getTransactionsSourceType(inputConfig);
        DataStream<String> transactionsStringsStream = initTransactionsSource(inputConfig, env)
                // todo: put below in initTransactionsSource method
                .name(transactionsSourceType.getName())
                .setParallelism(inputConfig.get(SOURCE_PARALLELISM));
        DataStream<Transaction> transactionsStream = stringsStreamToTransactions(transactionsStringsStream);
        return transactionsStream.assignTimestampsAndWatermarks(
                new SimpleBoundedOutOfOrdernessTimestampExtractor<>(inputConfig.get(OUT_OF_ORDERNESS)));
    }
}
