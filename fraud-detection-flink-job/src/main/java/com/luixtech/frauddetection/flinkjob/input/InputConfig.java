package com.luixtech.frauddetection.flinkjob.input;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputConfig {

    private final Map<InputParam<?>, Object> values = new HashMap<>();

    public <T> void put(InputParam<T> key, T value) {
        values.put(key, value);
    }

    public <T> T get(InputParam<T> key) {
        return key.getType().cast(values.get(key));
    }

    public <T> InputConfig(
            Parameters inputParams,
            List<InputParam<String>> stringInputParams,
            List<InputParam<Integer>> intInputParams,
            List<InputParam<Boolean>> boolInputParams) {
        overrideDefaults(inputParams, stringInputParams);
        overrideDefaults(inputParams, intInputParams);
        overrideDefaults(inputParams, boolInputParams);
    }

    public static InputConfig fromParameters(Parameters parameters) {
        return new InputConfig(
                parameters, Parameters.STRING_INPUT_PARAMS, Parameters.INT_INPUT_PARAMS, Parameters.BOOL_INPUT_PARAMS);
    }

    private <T> void overrideDefaults(Parameters inputParams, List<InputParam<T>> params) {
        for (InputParam<T> inputParam : params) {
            put(inputParam, inputParams.getOrDefault(inputParam));
        }
    }
}
