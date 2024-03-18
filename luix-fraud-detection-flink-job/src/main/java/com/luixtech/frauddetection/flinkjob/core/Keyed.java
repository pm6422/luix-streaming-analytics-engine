package com.luixtech.frauddetection.flinkjob.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Keyed<IN, ID, KEY> {
    /**
     * Transaction
     */
    private IN  wrapped;
    /**
     * The id of the rule that matched the input record
     */
    private ID  id;
    /**
     * Keys used to partition the input record, e.g: {payeeId=9905;beneficiaryId=29926}
     */
    private KEY key;
}
