package com.luixtech.frauddetection.flinkjob.input;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class Arguments {
    @Parameter(names = "--flink.dashboard.enabled", description = "Start Flink dashboard", arity = 1)
    public boolean flinkServerEnabled         = false;
    @Parameter(names = "--message.channel", description = "Message channel: kafka/socket")
    public  String  messageChannel             = "kafka";
    @Parameter(names = "-kafkaHost", description = "Kafka host")
    public  String  kafkaHost                  = "localhost";
    @Parameter(names = "-kafkaPort", description = "Kafka port")
    public  Integer kafkaPort                  = 9092;
    @Parameter(names = "-ruleSocketPort", description = "Socket port for rules import")
    public  Integer ruleSocketPort             = 9999;
    @Parameter(names = "-kafkaOffset", description = "Kafka offset")
    public  String  kafkaOffset                = "latest";
    @Parameter(names = "-transactionTopic", description = "Transaction topic")
    public  String  transactionTopic           = "transactions";
    @Parameter(names = "-ruleTopic", description = "Rule topic")
    public  String  ruleTopic                  = "rules";
    @Parameter(names = "-currentRuleTopic", description = "Current rule topic")
    public  String  currentRuleTopic           = "current-rules";
    @Parameter(names = "-latencyTopic", description = "Latency topic")
    public  String  latencyTopic               = "latency";
    @Parameter(names = "-alertTopic", description = "Alert topic")
    public  String  alertTopic                 = "alerts";
    @Parameter(names = "-sourceParallelism", description = "Parallelism for transaction source")
    public  Integer sourceParallelism          = 2;
    @Parameter(names = "-checkpointsEnabled", description = "Enables checkpointing for the streaming job. The distributed state of the streaming dataflow will be periodically snapshotted. In case of a failure, the streaming dataflow will be restarted from the latest completed checkpoint")
    public  boolean checkpointsEnabled         = false;
    @Parameter(names = "-checkpointInterval", description = "Time interval between state checkpoints in milliseconds")
    public  Integer checkpointInterval         = 60_000_0;
    @Parameter(names = "-minPauseBetweenCheckpoints", description = "The minimal pause before the next checkpoint is triggered")
    public  Long    minPauseBetweenCheckpoints = 10000L;
    @Parameter(names = "-outOfOrderdness", description = "")
    public  Integer outOfOrderdness            = 500;
    @Parameter(names = "-recordsPerSecond", description = "Max records per second for transaction generator")
    public  Integer recordsPerSecond           = 2;

    //    @Parameter
//    public List<String> parameters = new ArrayList<>();

}