package cn.luixtech.dae.common.rule;

public enum ArithmeticOperator {
    EQUAL,
    NOT_EQUAL,
    GREATER,
    LESS,
    GREATER_EQUAL,
    LESS_EQUAL;

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
                throw new IllegalArgumentException("Unsupported operator: " + this);
        }
    }
}