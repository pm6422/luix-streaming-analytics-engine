package com.luixtech.frauddetection.flinkjob.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Alert<E, V> {
    private Integer ruleId;
    private Rule    violatedRule;
    private String  key;
    private E       triggeringEvent;
    private V       triggeringValue;
}
