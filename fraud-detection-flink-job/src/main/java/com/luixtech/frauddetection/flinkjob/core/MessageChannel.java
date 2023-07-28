package com.luixtech.frauddetection.flinkjob.core;

import com.luixtech.utilities.lang.EnumValueHoldable;

public enum MessageChannel implements EnumValueHoldable<String> {
    KAFKA("kafka"),
    SOCKET("socket");

    private final String value;

    MessageChannel(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }
}