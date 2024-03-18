package com.luixtech.frauddetection.common.rule;

import java.util.List;

public interface BaseRule {
    void setId(String id);

    String getId();

    void setOperator(Operator operator);

    Operator getOperator();

    void setGroupingKeys(List<String> groupingKeys);

    List<String> getGroupingKeys();
}
