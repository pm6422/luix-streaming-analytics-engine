package com.luixtech.frauddetection.flinkjob.generator;

import com.luixtech.frauddetection.common.input.Input;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

public class InputGenerator extends BaseGenerator<Input> {
    private static final String PAYMENT_TYPE_CSH = "CSH";
    private static final String PAYMENT_TYPE_CRD = "CRD";
    private static final long   MAX_PAYEE_ID       = 100000;
    private static final long   MAX_BENEFICIARY_ID = 100000;
    private static final double MIN_PAYMENT_AMOUNT = 5d;
    private static final double MAX_PAYMENT_AMOUNT = 20d;

    public InputGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    public Input randomOne(SplittableRandom rnd, long id) {
        long recordId = rnd.nextLong(Long.MAX_VALUE);
        long payeeId = rnd.nextLong(MAX_PAYEE_ID);
        long beneficiaryId = rnd.nextLong(MAX_BENEFICIARY_ID);
        double paymentAmountDouble =
                ThreadLocalRandom.current().nextDouble(MIN_PAYMENT_AMOUNT, MAX_PAYMENT_AMOUNT);
        paymentAmountDouble = Math.floor(paymentAmountDouble * 100) / 100;
        BigDecimal paymentAmount = BigDecimal.valueOf(paymentAmountDouble);

        Map<String, Object> record = new HashMap<>();
        record.put("payeeId", payeeId);
        record.put("beneficiaryId", beneficiaryId);
        record.put("paymentAmount", paymentAmount);
        record.put("paymentType", paymentType(recordId));

        return Input.builder()
                .recordId(String.valueOf(recordId))
                .createdTime(System.currentTimeMillis())
                .record(record)
                .build();
    }

    private String paymentType(long id) {
        int name = (int) (id % 2);
        switch (name) {
            case 0:
                return PAYMENT_TYPE_CRD;
            case 1:
                return PAYMENT_TYPE_CSH;
            default:
                throw new IllegalStateException("");
        }
    }
}
