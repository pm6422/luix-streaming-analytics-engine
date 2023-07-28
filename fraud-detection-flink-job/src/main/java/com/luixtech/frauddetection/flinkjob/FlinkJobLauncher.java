package com.luixtech.frauddetection.flinkjob;

import com.luixtech.frauddetection.flinkjob.input.param.Parameters;
import com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions;
import com.luixtech.frauddetection.flinkjob.core.RulesEvaluator;

/**
 * Main class to launch the Flink job with CLI params:
 * --local-webserver true --data-source kafka --rules-source kafka --alerts-sink kafka --current-rules-sink kafka --latency-sink kafka --kafka-port 9092
 */
public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        ParameterDefinitions definitions = ParameterDefinitions.fromArgs(args);
        Parameters parameters = Parameters.fromDefinitions(definitions);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(parameters);
        rulesEvaluator.run();
    }
}
