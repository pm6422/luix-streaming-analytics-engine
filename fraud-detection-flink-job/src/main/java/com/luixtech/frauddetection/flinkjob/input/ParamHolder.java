package com.luixtech.frauddetection.flinkjob.input;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParamHolder {

    private static final Map<InputParam<?>, Object> HOLDER = new HashMap<>();

    public <T> ParamHolder(
            Parameters parameters,
            List<InputParam<String>> stringInputParams,
            List<InputParam<Integer>> intInputParams,
            List<InputParam<Boolean>> boolInputParams) {
        overrideDefaults(parameters, stringInputParams);
        overrideDefaults(parameters, intInputParams);
        overrideDefaults(parameters, boolInputParams);
    }

    public static ParamHolder fromParameters(Parameters parameters) {
        return new ParamHolder(
                parameters, Parameters.STRING_INPUT_PARAMS, Parameters.INT_INPUT_PARAMS, Parameters.BOOL_INPUT_PARAMS);
    }

    private <T> void overrideDefaults(Parameters parameters, List<InputParam<T>> inputParams) {
        for (InputParam<T> inputParam : inputParams) {
            put(inputParam, parameters.getOrDefaultValue(inputParam));
        }
    }

    private <T> void put(InputParam<T> inputParam, T value) {
        HOLDER.put(inputParam, value);
    }

    public <T> T getValue(InputParam<T> key) {
        return key.getType().cast(HOLDER.get(key));
    }
}
