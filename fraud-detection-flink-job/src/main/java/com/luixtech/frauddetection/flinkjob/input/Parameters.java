package com.luixtech.frauddetection.flinkjob.input;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Parameters {

    private static final Map<InputParam<?>, Object> HOLDER = new HashMap<>();

    public <T> Parameters(
            ParameterDefinitions parameterDefinitions,
            List<InputParam<String>> stringInputParams,
            List<InputParam<Integer>> intInputParams,
            List<InputParam<Boolean>> boolInputParams) {
        overrideDefaults(stringInputParams, parameterDefinitions);
        overrideDefaults(intInputParams, parameterDefinitions);
        overrideDefaults(boolInputParams, parameterDefinitions);

        HOLDER.forEach((key, value) -> log.info("{} = {}", key.getName(), value));
        System.out.println();
    }

    public static Parameters fromDefinitions(ParameterDefinitions parameterDefinitions) {
        return new Parameters(
                parameterDefinitions, ParameterDefinitions.STRING_INPUT_PARAMS, ParameterDefinitions.INT_INPUT_PARAMS, ParameterDefinitions.BOOL_INPUT_PARAMS);
    }

    private <T> void overrideDefaults(List<InputParam<T>> inputParams, ParameterDefinitions parameterDefinitions) {
        for (InputParam<T> inputParam : inputParams) {
            HOLDER.put(inputParam, parameterDefinitions.getOrDefaultValue(inputParam));
        }
    }

    public <T> T getValue(InputParam<T> key) {
        return key.getType().cast(HOLDER.get(key));
    }
}
