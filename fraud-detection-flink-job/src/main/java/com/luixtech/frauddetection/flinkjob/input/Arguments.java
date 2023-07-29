package com.luixtech.frauddetection.flinkjob.input;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class Arguments {
    @Parameter(names = "--flink.dashboard.enabled", description = "Start Flink dashboard", arity = 1)
    public  boolean flinkServerEnabled         = false;
    @Parameter(names = "--message.channel", description = "Message channel: kafka/socket")
    public  String  messageChannel             = "kafka";
    @Parameter(names = "--kafka.host", description = "Kafka host")
    public  String  kafkaHost                  = "localhost";
    @Parameter(names = "--kafka.port", description = "Kafka port")
    public  Integer kafkaPort                  = 9092;
    @Parameter(names = "--socket.rule-port", description = "Socket port for rules import")
    public  Integer ruleSocketPort             = 9999;
    @Parameter(names = "--kafka.offset", description = "Kafka offset")
    public  String  kafkaOffset                = "latest";
    @Parameter(names = "--kafka.topic.transaction", description = "Transaction topic")
    public  String  transactionTopic           = "transactions";
    @Parameter(names = "--kafka.topic.rule", description = "Rule topic")
    public  String  ruleTopic                  = "rules";
    @Parameter(names = "--kafka.topic.current-rule", description = "Current rule topic")
    public  String  currentRuleTopic           = "current-rules";
    @Parameter(names = "--kafka.topic.latency", description = "Latency topic")
    public  String  latencyTopic               = "latency";
    @Parameter(names = "--kafka.topic.alert", description = "Alert topic")
    public  String  alertTopic                 = "alerts";
    @Parameter(names = "--source-parallelism", description = "Parallelism for transaction source")
    public  Integer sourceParallelism          = 2;
    @Parameter(names = "--checkpoints.enabled", description = "Enables checkpointing for the streaming job. The distributed state of the streaming dataflow will be periodically snapshotted. In case of a failure, the streaming dataflow will be restarted from the latest completed checkpoint", arity = 1)
    public  boolean checkpointsEnabled         = false;
    @Parameter(names = "--checkpoint.interval", description = "Time interval between state checkpoints in milliseconds")
    public  Integer checkpointInterval         = 60_000_0;
    @Parameter(names = "--checkpoint.min-pause", description = "The minimal pause before the next checkpoint is triggered")
    public  Long    minPauseBetweenCheckpoints = 10000L;
    @Parameter(names = "--out-of-orderdness", description = "")
    public  Integer outOfOrderdness            = 500;
    @Parameter(names = "--generator.records-per-second", description = "Max records per second for transaction generator")
    public  Integer recordsPerSecond           = 2;
}