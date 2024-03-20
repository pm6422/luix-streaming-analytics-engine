package cn.luixtech.cae.flinkjob.core;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class Arguments {
    public static final String  CHANNEL_KAFKA              = "kafka";
    public static final String  CHANNEL_SOCKET             = "socket";
    @Parameter(names = "--flink.dashboard.enabled", description = "Start flink dashboard", arity = 1)
    public              boolean flinkServerEnabled         = true;
    @Parameter(names = "--message.channel", description = "Message channel: kafka/socket")
    public              String  messageChannel             = CHANNEL_KAFKA;
    @Parameter(names = "--kafka.host", description = "Kafka host")
    public              String  kafkaHost                  = "localhost";
    @Parameter(names = "--kafka.port", description = "Kafka port")
    public              Integer kafkaPort                  = 9092;
    @Parameter(names = "--socket.rule-port", description = "Socket port for rules import")
    public              Integer ruleSocketPort             = 9999;
    @Parameter(names = "--kafka.topic.input", description = "Input topic")
    public              String  inputTopic                 = "input";
    @Parameter(names = "--kafka.topic.input.group", description = "Input topic group")
    public              String  inputTopicGroup            = "inputGrp";
    @Parameter(names = "--kafka.topic.rule", description = "Rule topic")
    public              String  ruleTopic                  = "rule";
    @Parameter(names = "--kafka.topic.rule.group", description = "Rule topic group")
    public              String  ruleTopicGroup             = "ruleGrp";
    @Parameter(names = "--kafka.topic.latency", description = "Latency topic")
    public              String  latencyTopic               = "latency";
    @Parameter(names = "--kafka.topic.output", description = "Output topic")
    public              String  outputTopic                = "output";
    @Parameter(names = "--input-source-parallelism", description = "Parallelism for input source")
    public              Integer inputSourceParallelism     = 2;
    @Parameter(names = "--checkpoints.enabled", description = "Enables checkpointing for the streaming job. The distributed state of the streaming dataflow will be periodically snapshot. In case of a failure, the streaming dataflow will be restarted from the latest completed checkpoint", arity = 1)
    public              boolean checkpointsEnabled         = false;
    @Parameter(names = "--checkpoint.interval", description = "Time interval between state checkpoints in milliseconds")
    public              Integer checkpointInterval         = 60_000_0;
    @Parameter(names = "--checkpoint.min-pause", description = "The minimal pause before the next checkpoint is triggered")
    public              Long    minPauseBetweenCheckpoints = 10000L;
    @Parameter(names = "--out-of-orderdness", description = "")
    public              Integer outOfOrderdness            = 500;
    @Parameter(names = "--generator.inputs-per-second", description = "Max records per second for input generator")
    public              Integer inputsPerSecond            = 2;
}