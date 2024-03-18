package com.luixtech.frauddetection.common.input;

import lombok.Data;

import java.util.Map;

@Data
public class InputLog {

    private String recordId;

    private Map<String, Object> record;

    private Map<String, Object> recordState;

}