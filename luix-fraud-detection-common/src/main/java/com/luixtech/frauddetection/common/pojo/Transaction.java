package com.luixtech.frauddetection.common.pojo;

import com.luixtech.frauddetection.common.pojo.base.IngestionTimeAssignable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction implements IngestionTimeAssignable {
    public  long        transactionId;
    /**
     * Generation timestamp of the transaction event, unit: milliseconds
     */
    public  long        eventTime;
    /**
     * Timestamp of ingestion into the flink input source, unit: milliseconds
     */
    private Long        ingestionTime;
    public  long        payeeId;
    public  long        beneficiaryId;
    public  BigDecimal  paymentAmount;
    public  PaymentType paymentType;

    private static DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZone(ZoneOffset.UTC);

    public enum PaymentType {
        CSH("CSH"),
        CRD("CRD");

        String representation;

        PaymentType(String repr) {
            this.representation = repr;
        }

        public static PaymentType fromString(String representation) {
            for (PaymentType b : PaymentType.values()) {
                if (b.representation.equals(representation)) {
                    return b;
                }
            }
            return null;
        }
    }

    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTime;
    }
}
