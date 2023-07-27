package com.luixtech.frauddetection.flinkjob;

import com.luixtech.frauddetection.flinkjob.input.InputConfig;
import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.transaction.rule.RulesEvaluator;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Main class to launch the Flink job with CLI params:
 * --local true --data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka --latency-sink kafka --kafka-port 9092
 */
public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Parameters inputParams = new Parameters(parameterTool);
        InputConfig inputConfig = new InputConfig(inputParams, Parameters.STRING_INPUT_PARAMS, Parameters.INT_INPUT_PARAMS, Parameters.BOOL_INPUT_PARAMS);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(inputConfig);
        rulesEvaluator.run();
    }
}
