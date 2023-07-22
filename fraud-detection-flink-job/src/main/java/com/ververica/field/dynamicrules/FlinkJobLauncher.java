package com.ververica.field.dynamicrules;

import com.ververica.field.config.Config;
import com.ververica.field.config.Parameters;
import org.apache.flink.api.java.utils.ParameterTool;

import static com.ververica.field.config.Parameters.*;

public class FlinkJobLauncher {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        Parameters inputParams = new Parameters(tool);
        Config config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS);
        RulesEvaluator rulesEvaluator = new RulesEvaluator(config);
        rulesEvaluator.run();
    }
}
