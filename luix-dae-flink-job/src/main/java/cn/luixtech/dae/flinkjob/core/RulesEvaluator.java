package cn.luixtech.dae.flinkjob.core;

import cn.luixtech.dae.common.output.Output;
import cn.luixtech.dae.common.input.Input;
import cn.luixtech.dae.common.rule.RuleCommand;
import cn.luixtech.dae.flinkjob.output.OutputSink;
import cn.luixtech.dae.flinkjob.output.LatencySink;
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

import static cn.luixtech.dae.flinkjob.core.Arguments.CHANNEL_KAFKA;
import static cn.luixtech.dae.flinkjob.core.Arguments.CHANNEL_SOCKET;
import static cn.luixtech.dae.flinkjob.input.RuleSource.initRuleCommandSource;
import static cn.luixtech.dae.flinkjob.input.RuleSource.stringStreamToRuleCommand;
import static cn.luixtech.dae.flinkjob.input.InputSource.initInputSource;
import static cn.luixtech.dae.flinkjob.input.InputSource.stringStreamToInput;

@Slf4j
@AllArgsConstructor
public class RulesEvaluator {

    private final Arguments arguments;

    public void run() throws Exception {
        StreamExecutionEnvironment env = createStreamExecutionEnv();

        BroadcastStream<RuleCommand> broadcastRuleCommandStream = createBroadcastRuleCommandStream(env);
        DataStream<Input> inputStream = createInputStream(env);

        // processing pipeline setup
        SingleOutputStreamOperator<Output> outputStream = inputStream
                .connect(broadcastRuleCommandStream)
                .process(new InputShardingFunction())
                .uid(InputShardingFunction.class.getSimpleName())
                .name("Input sharding function")
                // cannot be optimized to lambda
                .keyBy((shardingPolicy) -> shardingPolicy.getShardingKey())
                .connect(broadcastRuleCommandStream)
                .process(new RuleEvaluationFunction())
                .uid(RuleEvaluationFunction.class.getSimpleName())
                .name("Rule evaluation function");

        outputStream.print().name("Output STDOUT sink");

        DataStream<String> outputStringStream = OutputSink.outputStreamToString(outputStream);
        DataStreamSink<String> outputSink = OutputSink.addOutputSink(arguments, outputStringStream);
        outputSink.setParallelism(1).name("Output JSON sink");

        DataStream<String> ruleEvaluationResultStream = outputStream.getSideOutput(Descriptors.RULE_EVALUATION_RESULT_TAG);
        ruleEvaluationResultStream.print().setParallelism(1).name("Rule evaluation result sink");

        DataStream<Long> handlingLatency = outputStream.getSideOutput(Descriptors.HANDLING_LATENCY_SINK_TAG);
        DataStream<String> latencies = handlingLatency.timeWindowAll(Time.seconds(10)).aggregate(new AverageAggregate()).map(String::valueOf);
        DataStreamSink<String> latencySink = LatencySink.addLatencySink(arguments, latencies);
        latencySink.name("Handling Latency Sink");

        env.execute("Content analytic engine");
    }

    private StreamExecutionEnvironment createStreamExecutionEnv() {
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
                // env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    private BroadcastStream<RuleCommand> createBroadcastRuleCommandStream(StreamExecutionEnvironment env) {
        DataStream<RuleCommand> ruleCommandStream = createRuleCommandStream(env);
        // Broadcast rules to all flink servers on the same cluster
        return ruleCommandStream.broadcast(Descriptors.RULES_COMMAND_DESCRIPTOR);
    }

    private DataStream<RuleCommand> createRuleCommandStream(StreamExecutionEnvironment env) {
        DataStream<String> ruleCommandStringStream = initRuleCommandSource(arguments, env);
        return stringStreamToRuleCommand(arguments, ruleCommandStringStream);
    }

    private DataStream<Input> createInputStream(StreamExecutionEnvironment env) {
        DataStream<String> inputStringStream = initInputSource(arguments, env);
        return stringStreamToInput(arguments, inputStringStream);
    }
}
