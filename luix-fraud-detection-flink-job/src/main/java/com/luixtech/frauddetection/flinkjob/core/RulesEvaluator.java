package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.frauddetection.common.alert.Alert;
import com.luixtech.frauddetection.common.input.InputRecord;
import com.luixtech.frauddetection.common.rule.RuleCommand;
import com.luixtech.frauddetection.flinkjob.core.function.AverageAggregate;
import com.luixtech.frauddetection.flinkjob.output.AlertSink;
import com.luixtech.frauddetection.flinkjob.output.LatencySink;
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

import static com.luixtech.frauddetection.flinkjob.core.Arguments.CHANNEL_KAFKA;
import static com.luixtech.frauddetection.flinkjob.core.Arguments.CHANNEL_SOCKET;
import static com.luixtech.frauddetection.flinkjob.input.RuleSource.initRulesSource;
import static com.luixtech.frauddetection.flinkjob.input.RuleSource.stringsStreamToRules;
import static com.luixtech.frauddetection.flinkjob.input.InputRecordSource.initTransactionsSource;
import static com.luixtech.frauddetection.flinkjob.input.InputRecordSource.stringsStreamToTransactions;

@Slf4j
@AllArgsConstructor
public class RulesEvaluator {

    private final Arguments arguments;

    public void run() throws Exception {
        // Create stream execution environment
        StreamExecutionEnvironment env = createExecutionEnv();
        DataStream<RuleCommand> ruleStream = createRuleStream(env);
        // Rule must be broadcast to all flink servers on the same cluster
        BroadcastStream<RuleCommand> broadcastRuleStream = ruleStream.broadcast(Descriptors.RULES_DESCRIPTOR);
        DataStream<InputRecord> inputRecordStream = createInputRecordStream(env);

        // Processing pipeline setup
        DataStream<Alert> alertStream = inputRecordStream
                .connect(broadcastRuleStream)
                .process(new DynamicKeyFunction())
                .uid(DynamicKeyFunction.class.getSimpleName())
                .name("Dynamic Partitioning Function")
                // cannot be optimized to lambda
                .keyBy((keyed) -> keyed.getKey())
                .connect(broadcastRuleStream)
                .process(new DynamicAlertFunction())
                .uid(DynamicAlertFunction.class.getSimpleName())
                .name("Dynamic Rule Evaluation Function");

        alertStream.print().name("Alert STDOUT Sink");
        DataStream<String> alertsJson = AlertSink.alertsStreamToJson(alertStream);
        DataStreamSink<String> alertSink = AlertSink.addAlertsSink(arguments, alertsJson);
        alertSink.setParallelism(1).name("Alerts JSON Sink");

        DataStream<String> allRuleEvaluations = ((SingleOutputStreamOperator<Alert>) alertStream).getSideOutput(Descriptors.RULE_EVALUATION_RESULT_TAG);
        allRuleEvaluations.print().setParallelism(1).name("Rule Evaluation Sink");

        DataStream<Long> handlingLatency = ((SingleOutputStreamOperator<Alert>) alertStream).getSideOutput(Descriptors.HANDLING_LATENCY_SINK_TAG);
        DataStream<String> latencies = handlingLatency.timeWindowAll(Time.seconds(10)).aggregate(new AverageAggregate()).map(String::valueOf);
        DataStreamSink<String> latencySink = LatencySink.addLatencySink(arguments, latencies);
        latencySink.name("Handling Latency Sink");

        env.execute("Fraud Detection Engine");
    }

    private StreamExecutionEnvironment createExecutionEnv() {
        StreamExecutionEnvironment env = getStreamExecutionEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        if (arguments.checkpointsEnabled) {
            env.enableCheckpointing(arguments.checkpointInterval);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(arguments.minPauseBetweenCheckpoints);
        }
        configureRestartStrategy(env);
        return env;
    }

    private StreamExecutionEnvironment getStreamExecutionEnv() {
        if (!arguments.flinkServerEnabled) {
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
        switch (arguments.messageChannel) {
            case CHANNEL_SOCKET:
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,
                        org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
                break;
            case CHANNEL_KAFKA:
                // Default - unlimited restart strategy.
                //        env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    private DataStream<RuleCommand> createRuleStream(StreamExecutionEnvironment env) {
        DataStream<String> rulesStringStream = initRulesSource(arguments, env);
        return stringsStreamToRules(arguments, rulesStringStream);
    }

    private DataStream<InputRecord> createInputRecordStream(StreamExecutionEnvironment env) {
        DataStream<String> inputRecordsStringsStream = initTransactionsSource(arguments, env);
        return stringsStreamToTransactions(arguments, inputRecordsStringsStream);
    }
}
