package com.luixtech.frauddetection.common.rule;

import java.util.Arrays;

public enum Operator {
    EQUAL("=="),
    NOT_EQUAL("!="),
    GREATER(">"),
    LESS("<"),
    GREATER_EQUAL(">="),
    LESS_EQUAL("<=");

    private final String operator;

    Operator(String operator) {
        this.operator = operator;
    }

    public static Operator fromValue(String value) {
        return Arrays.stream(Operator.values())
                .filter(e -> e.operator.equals(value))
                .findFirst()
                .orElse(null);
    }
}