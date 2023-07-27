package com.luixtech.frauddetection.flinkjob.input;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Parameters {

    private static final Map<InputParam<?>, Object> HOLDER = new HashMap<>();

    <T> Parameters(ParameterDefinitions parameterDefinitions, List<InputParam<?>> all) {
        overrideDefaults(all, parameterDefinitions);
        HOLDER.forEach((key, value) -> log.info("{} = {}", key.getName(), value));
    }

    public static Parameters fromDefinitions(ParameterDefinitions parameterDefinitions) {
        return new Parameters(parameterDefinitions, ParameterDefinitions.ALL);
    }

    private <T> void overrideDefaults(List<InputParam<?>> inputParams, ParameterDefinitions parameterDefinitions) {
        for (InputParam<?> inputParam : inputParams) {
            HOLDER.put(inputParam, parameterDefinitions.getOrDefaultValue(inputParam));
        }
    }

    public <T> T getValue(InputParam<T> key) {
        return key.getType().cast(HOLDER.get(key));
    }
}
