package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.flinkjob.core.function.AverageAggregate;
import com.luixtech.frauddetection.flinkjob.domain.Alert;
import com.luixtech.frauddetection.flinkjob.domain.Rule;
import com.luixtech.frauddetection.flinkjob.input.param.Parameters;
import com.luixtech.frauddetection.flinkjob.input.source.RulesSource;
import com.luixtech.frauddetection.flinkjob.output.sinks.AlertsSink;
import com.luixtech.frauddetection.flinkjob.output.sinks.CurrentRulesSink;
import com.luixtech.frauddetection.flinkjob.output.sinks.LatencySink;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

import static com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions.*;
import static com.luixtech.frauddetection.flinkjob.input.source.RulesSource.*;
import static com.luixtech.frauddetection.flinkjob.input.source.TransactionsSource.initTransactionsSource;
import static com.luixtech.frauddetection.flinkjob.input.source.TransactionsSource.stringsStreamToTransactions;

@Slf4j
@AllArgsConstructor
public class RulesEvaluator {

    private final Parameters parameters;

    public void run() throws Exception {
        // Create stream execution environment
        StreamExecutionEnvironment env = createExecutionEnv();

        DataStream<Rule> ruleStream = createRuleStream(env);
        // Rule must be broadcast to all flink servers on the same cluster
        BroadcastStream<Rule> broadcastRuleStream = ruleStream.broadcast(Descriptors.RULES_DESCRIPTOR);
        DataStream<Transaction> transactionStream = createTransactionStream(env);

        // Processing pipeline setup
        DataStream<Alert> alertStream = transactionStream
                .connect(broadcastRuleStream)
                .process(new DynamicKeyFunction())
                .uid("DynamicKeyFunction")
                .name("Dynamic Partitioning Function")
                // cannot be optimized by lambda
                .keyBy((keyed) -> keyed.getKey())
                .connect(broadcastRuleStream)
                .process(new DynamicAlertFunction())
                .uid("DynamicAlertFunction")
                .name("Dynamic Rule Evaluation Function");

        DataStream<String> allRuleEvaluations = ((SingleOutputStreamOperator<Alert>) alertStream).getSideOutput(Descriptors.DEMO_SINK_TAG);
        DataStream<Long> latency = ((SingleOutputStreamOperator<Alert>) alertStream).getSideOutput(Descriptors.LATENCY_SINK_TAG);
        DataStream<Rule> currentRules = ((SingleOutputStreamOperator<Alert>) alertStream).getSideOutput(Descriptors.CURRENT_RULES_SINK_TAG);

        alertStream.print().name("Alert STDOUT Sink");
        allRuleEvaluations.print().setParallelism(1).name("Rule Evaluation Sink");

        DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alertStream);
        DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);

        currentRulesJson.print();

        DataStreamSink<String> alertsSink = AlertsSink.addAlertsSink(parameters, alertsJson);
        alertsSink.setParallelism(1).name("Alerts JSON Sink");

        DataStreamSink<String> currentRulesSink = CurrentRulesSink.addRulesSink(parameters, currentRulesJson);
        currentRulesSink.setParallelism(1);

        DataStream<String> latencies = latency
                .timeWindowAll(Time.seconds(10))
                .aggregate(new AverageAggregate())
                .map(String::valueOf);

        DataStreamSink<String> latencySink = LatencySink.addLatencySink(parameters, latencies);
        latencySink.name("Latency Sink");

        env.execute("Fraud Detection Engine");
    }

    private StreamExecutionEnvironment createExecutionEnv() {
        StreamExecutionEnvironment env = getStreamExecutionEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (parameters.getValue(ENABLE_CHECKPOINTS)) {
            env.enableCheckpointing(parameters.getValue(CHECKPOINT_INTERVAL));
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameters.getValue(MIN_PAUSE_BETWEEN_CHECKPOINTS));
        }
        configureRestartStrategy(env);
        return env;
    }

    private StreamExecutionEnvironment getStreamExecutionEnv() {
        if (!parameters.getValue(LOCAL_WEBSERVER)) {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }

        // Create an embedded Flink execution environment with flink UI dashboard
        Configuration flinkConfig = new Configuration();
        flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        // Fixed Insufficient number of network buffers on local linux env
        // Refer to https://stackoverflow.com/questions/49283934/flink-ioexception-insufficient-number-of-network-buffers
        flinkConfig.setString("taskmanager.memory.network.max", "2gb");
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
    }

    private void configureRestartStrategy(StreamExecutionEnvironment env) {
        RulesSource.Type rulesSourceType = getRuleSourceType(parameters);
        switch (rulesSourceType) {
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

    private DataStream<Rule> createRuleStream(StreamExecutionEnvironment env) {
        DataStream<String> rulesStringStream = initRulesSource(parameters, env);
        return stringsStreamToRules(parameters, rulesStringStream);
    }

    private DataStream<Transaction> createTransactionStream(StreamExecutionEnvironment env) {
        DataStream<String> transactionsStringsStream = initTransactionsSource(parameters, env);
        return stringsStreamToTransactions(parameters, transactionsStringsStream);
    }
}
