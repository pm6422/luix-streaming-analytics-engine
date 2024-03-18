package com.luixtech.frauddetection.flinkjob.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Keyed<IN, KEY, ID> {
    /**
     * Input record
     */
    private IN  inputRecord;
    /**
     * Keys used to partition the input record, e.g: {tenant=tesla;model=X9}
     */
    private KEY groupKeys;
    /**
     * The id of the rule that matched the input record
     */
    private ID  ruleId;
}
