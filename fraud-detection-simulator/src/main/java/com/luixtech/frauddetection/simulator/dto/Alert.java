package com.luixtech.frauddetection.simulator.dto;

import com.luixtech.frauddetection.common.dto.Transaction;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class Alert {
    private Integer     ruleId;
    private String      rulePayload;
    private Transaction triggeringEvent;
    private BigDecimal  triggeringValue;
}
