package com.luixtech.frauddetection.flinkjob;

import com.luixtech.frauddetection.flinkjob.core.RulesEvaluator;
import com.luixtech.frauddetection.flinkjob.input.Arguments;
import com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions;
import com.luixtech.frauddetection.flinkjob.input.param.Parameters;

/**
 * Main class to launch the Flink job with CLI params:
 * --message-channel socket --flink-server true
 */
public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        ParameterDefinitions definitions = ParameterDefinitions.fromArgs(args);
        Parameters parameters = Parameters.fromDefinitions(definitions);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(arguments, parameters);
        rulesEvaluator.run();
    }
}
