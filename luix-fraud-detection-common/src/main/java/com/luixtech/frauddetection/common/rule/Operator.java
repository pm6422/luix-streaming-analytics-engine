package com.luixtech.frauddetection.common.rule;

public enum Operator {
    EQUAL("="),
    NOT_EQUAL("!="),
    GREATER_EQUAL(">="),
    LESS_EQUAL("<="),
    GREATER(">"),
    LESS("<");

    private final String operator;

    Operator(String operator) {
        this.operator = operator;
    }

    public static Operator fromString(String text) {
        for (Operator v : Operator.values()) {
            if (v.operator.equals(text)) {
                return v;
            }
        }
        return null;
    }
}