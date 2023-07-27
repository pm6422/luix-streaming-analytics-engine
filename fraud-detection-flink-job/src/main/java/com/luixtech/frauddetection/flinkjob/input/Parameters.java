package com.luixtech.frauddetection.flinkjob.input;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parameters {

    private static final Map<InputParam<?>, Object> HOLDER = new HashMap<>();

    public <T> Parameters(
            ParameterDefinitions parameterDefinitions,
            List<InputParam<String>> stringInputParams,
            List<InputParam<Integer>> intInputParams,
            List<InputParam<Boolean>> boolInputParams) {
        overrideDefaults(parameterDefinitions, stringInputParams);
        overrideDefaults(parameterDefinitions, intInputParams);
        overrideDefaults(parameterDefinitions, boolInputParams);
    }

    public static Parameters fromDefinitions(ParameterDefinitions parameterDefinitions) {
        return new Parameters(
                parameterDefinitions, ParameterDefinitions.STRING_INPUT_PARAMS, ParameterDefinitions.INT_INPUT_PARAMS, ParameterDefinitions.BOOL_INPUT_PARAMS);
    }

    private <T> void overrideDefaults(ParameterDefinitions parameterDefinitions, List<InputParam<T>> inputParams) {
        for (InputParam<T> inputParam : inputParams) {
            put(inputParam, parameterDefinitions.getOrDefaultValue(inputParam));
        }
    }

    private <T> void put(InputParam<T> inputParam, T value) {
        HOLDER.put(inputParam, value);
    }

    public <T> T getValue(InputParam<T> key) {
        return key.getType().cast(HOLDER.get(key));
    }
}
