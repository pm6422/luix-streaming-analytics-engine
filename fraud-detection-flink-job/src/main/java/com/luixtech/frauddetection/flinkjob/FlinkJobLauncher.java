package com.luixtech.frauddetection.flinkjob;

import com.luixtech.frauddetection.flinkjob.input.Config;
import com.luixtech.frauddetection.flinkjob.input.Parameters;
import com.luixtech.frauddetection.flinkjob.transaction.rule.RulesEvaluator;
import org.apache.flink.api.java.utils.ParameterTool;

public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        Parameters inputParams = new Parameters(tool);
        Config config = new Config(inputParams, Parameters.STRING_PARAMS, Parameters.INT_PARAMS, Parameters.BOOL_PARAMS);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(config);
        rulesEvaluator.run();
    }
}
