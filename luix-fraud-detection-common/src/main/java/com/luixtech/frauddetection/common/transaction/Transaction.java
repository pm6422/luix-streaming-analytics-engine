package com.luixtech.frauddetection.common.transaction;

import com.luixtech.frauddetection.common.IngestionTimeAssignable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction implements IngestionTimeAssignable {
    public static final String PAYMENT_TYPE_CSH = "CSH";
    public static final String PAYMENT_TYPE_CRD = "CRD";

    public  String     id;
    /**
     * Created timestamp of the transaction, unit: ms
     */
    public  long       createdTime;
    /**
     * Timestamp of ingestion into the flink input source, unit: ms
     */
    private Long       ingestionTime;
    public  long       payeeId;
    public  long       beneficiaryId;
    public  BigDecimal paymentAmount;
    public  String     paymentType;


    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTime;
    }
}
