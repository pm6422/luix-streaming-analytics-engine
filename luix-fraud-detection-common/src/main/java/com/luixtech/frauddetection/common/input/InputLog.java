package com.luixtech.frauddetection.common.input;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.Map;

@Data
public class InputLog {

    private String recordId;

    private Map<String, Object> record;

    private Map<String, Object> recordState;

}