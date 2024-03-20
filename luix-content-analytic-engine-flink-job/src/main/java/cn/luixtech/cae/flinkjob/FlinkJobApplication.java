package cn.luixtech.cae.flinkjob;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import cn.luixtech.cae.flinkjob.core.RulesEvaluator;
import cn.luixtech.cae.flinkjob.core.Arguments;

/**
 * Main class to launch the Flink job with CLI params example:
 * --flink.dashboard.enabled=true --message.channel=socket
 */
public class FlinkJobApplication {
    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander commander = JCommander.newBuilder().addObject(arguments).build();
        commander.usage();
        commander.parse(args);
        commander.getConsole().println("Starting Flink job with arguments: ");
        commander.getConsole().println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(arguments));
        RulesEvaluator rulesEvaluator = new RulesEvaluator(arguments);
        rulesEvaluator.run();
    }
}
