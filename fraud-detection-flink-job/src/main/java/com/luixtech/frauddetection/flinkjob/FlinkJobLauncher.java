package com.luixtech.frauddetection.flinkjob;

import com.luixtech.frauddetection.flinkjob.input.ParamHolder;
import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.transaction.rule.RulesEvaluator;

/**
 * Main class to launch the Flink job with CLI params:
 * --local true --data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka --latency-sink kafka --kafka-port 9092
 */
public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        Parameters parameters = Parameters.fromArgs(args);
        ParamHolder paramHolder = ParamHolder.fromParameters(parameters);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(paramHolder);
        rulesEvaluator.run();
    }
}
