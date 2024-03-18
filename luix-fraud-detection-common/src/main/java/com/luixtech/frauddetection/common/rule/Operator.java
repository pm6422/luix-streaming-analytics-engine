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

    public <T extends Comparable<T>> boolean compare(T actualValue, T expectedValue) {
        switch (this) {
            case EQUAL:
                return actualValue.compareTo(expectedValue) == 0;
            case NOT_EQUAL:
                return actualValue.compareTo(expectedValue) != 0;
            case GREATER:
                return actualValue.compareTo(expectedValue) > 0;
            case LESS:
                return actualValue.compareTo(expectedValue) < 0;
            case GREATER_EQUAL:
                return actualValue.compareTo(expectedValue) >= 0;
            case LESS_EQUAL:
                return actualValue.compareTo(expectedValue) <= 0;
            default:
                throw new IllegalArgumentException("Unknown operator: " + this);
        }
    }

    public static Operator fromValue(String value) {
        return Arrays.stream(Operator.values())
                .filter(e -> e.operator.equals(value))
                .findFirst()
                .orElse(null);
    }
}