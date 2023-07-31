package com.luixtech.frauddetection.flinkjob.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Keyed<IN, KEY, ID> {
    /**
     * Transaction
     */
    private IN  wrapped;
    /**
     * Keys used to partition the transaction, e.g: {payeeId=9905;beneficiaryId=29926}
     */
    private KEY key;
    /**
     * The id of the rule that matched the transaction
     */
    private ID  id;
}
