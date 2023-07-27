package com.luixtech.frauddetection.flinkjob;

import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.input.ParameterDefinitions;
import com.luixtech.frauddetection.flinkjob.transaction.rule.RulesEvaluator;

/**
 * Main class to launch the Flink job with CLI params:
 * --local true --data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka --latency-sink kafka --kafka-port 9092
 */
public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        ParameterDefinitions definitions = ParameterDefinitions.fromArgs(args);
        Parameters parameters = Parameters.fromDefinitions(definitions);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(parameters);
        rulesEvaluator.run();
    }
}
