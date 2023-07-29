package com.luixtech.frauddetection.flinkjob;

import com.beust.jcommander.JCommander;
import com.luixtech.frauddetection.flinkjob.core.RulesEvaluator;
import com.luixtech.frauddetection.flinkjob.input.Arguments;

/**
 * Main class to launch the Flink job with CLI params example:
 * --flink.server.enabled=true --message.channel=socket
 */
public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander
                .newBuilder()
                .addObject(arguments)
                .build()
                .parse(args);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(arguments);
        rulesEvaluator.run();
    }
}
