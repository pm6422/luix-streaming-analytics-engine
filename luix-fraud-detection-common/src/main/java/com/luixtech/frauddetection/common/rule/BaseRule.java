package com.luixtech.frauddetection.common.rule;

import lombok.Data;

import java.util.List;

@Data
public abstract class BaseRule {
    protected String       id;
    protected List<String> groupingKeys;
    protected Operator     operator;
}
