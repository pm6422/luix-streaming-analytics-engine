package com.luixtech.frauddetection.flinkjob.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Keyed<IN, ID, KEY> {
    /**
     * Input record
     */
    private IN  inputRecord;
    /**
     * The id of the rule that matched the input record
     */
    private ID  ruleId;
    /**
     * Keys used to partition the input record, e.g: {tenant=tesla;model=X9}
     */
    private KEY groupKeys;
}
