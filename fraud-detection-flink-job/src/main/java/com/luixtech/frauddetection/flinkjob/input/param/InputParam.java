package com.luixtech.frauddetection.flinkjob.input.param;

import lombok.Getter;

@Getter
public class InputParam<T> {
    private final String   name;
    private final Class<T> type;
    private final T        defaultValue;

    InputParam(String name, T defaultValue, Class<T> type) {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
    }

    public static InputParam<String> string(String name, String defaultValue) {
        return new InputParam<>(name, defaultValue, String.class);
    }

    public static InputParam<Integer> integer(String name, Integer defaultValue) {
        return new InputParam<>(name, defaultValue, Integer.class);
    }

    public static InputParam<Boolean> bool(String name, Boolean defaultValue) {
        return new InputParam<>(name, defaultValue, Boolean.class);
    }
}
