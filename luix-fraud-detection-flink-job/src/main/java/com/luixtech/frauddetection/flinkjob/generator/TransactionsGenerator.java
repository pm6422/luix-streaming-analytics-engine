package com.luixtech.frauddetection.flinkjob.generator;

import com.luixtech.frauddetection.common.pojo.Transaction;
import com.luixtech.frauddetection.common.pojo.Transaction.PaymentType;

import java.math.BigDecimal;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionsGenerator extends BaseGenerator<Transaction> {
    private static final long   MAX_PAYEE_ID       = 100000;
    private static final long   MAX_BENEFICIARY_ID = 100000;
    private static final double MIN_PAYMENT_AMOUNT = 5d;
    private static final double MAX_PAYMENT_AMOUNT = 20d;

    public TransactionsGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    public Transaction randomEvent(SplittableRandom rnd, long id) {
        long transactionId = rnd.nextLong(Long.MAX_VALUE);
        long payeeId = rnd.nextLong(MAX_PAYEE_ID);
        long beneficiaryId = rnd.nextLong(MAX_BENEFICIARY_ID);
        double paymentAmountDouble =
                ThreadLocalRandom.current().nextDouble(MIN_PAYMENT_AMOUNT, MAX_PAYMENT_AMOUNT);
        paymentAmountDouble = Math.floor(paymentAmountDouble * 100) / 100;
        BigDecimal paymentAmount = BigDecimal.valueOf(paymentAmountDouble);

        return Transaction.builder()
                .transactionId(transactionId)
                .payeeId(payeeId)
                .beneficiaryId(beneficiaryId)
                .paymentAmount(paymentAmount)
                .paymentType(paymentType(transactionId))
                .eventTime(System.currentTimeMillis())
                .ingestionTime(System.currentTimeMillis())
                .build();
    }

    private PaymentType paymentType(long id) {
        int name = (int) (id % 2);
        switch (name) {
            case 0:
                return PaymentType.CRD;
            case 1:
                return PaymentType.CSH;
            default:
                throw new IllegalStateException("");
        }
    }
}
