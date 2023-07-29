package com.luixtech.frauddetection.flinkjob;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luixtech.frauddetection.flinkjob.core.RulesEvaluator;
import com.luixtech.frauddetection.flinkjob.input.Arguments;

/**
 * Main class to launch the Flink job with CLI params example:
 * --flink.dashboard.enabled=true --message.channel=socket
 */
public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander commander = JCommander
                .newBuilder()
                .addObject(arguments)
                .build();
        commander.usage();
        commander.parse(args);
        commander.getConsole().println("Starting Flink job with arguments: ");
        commander.getConsole().println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(arguments));
        RulesEvaluator rulesEvaluator = new RulesEvaluator(arguments);
        rulesEvaluator.run();
    }
}
