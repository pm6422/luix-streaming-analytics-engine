package com.luixtech.frauddetection.simulator.generator;

import com.luixtech.frauddetection.common.input.InputRecord;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.SplittableRandom;
import java.util.function.Consumer;

@Slf4j
public class InputRecordGenerator extends AbstractTransactionsGenerator {

    private       long       lastPayeeIdBeneficiaryIdTriggered = System.currentTimeMillis();
    private       long       lastBeneficiaryIdTriggered        = System.currentTimeMillis();
    private final BigDecimal beneficiaryLimit                  = new BigDecimal(10000000);
    private final BigDecimal payeeBeneficiaryLimit             = new BigDecimal(20000000);

    public InputRecordGenerator(Consumer<InputRecord> inputRecordProducer, int maxRecordsPerSecond) {
        super(inputRecordProducer, maxRecordsPerSecond);
    }

    @Override
    protected InputRecord randomOne(SplittableRandom rnd, Long eventTime) {
        InputRecord input = super.randomOne(rnd, eventTime);
        long now = System.currentTimeMillis();
        if (now - lastBeneficiaryIdTriggered > 8000 + rnd.nextInt(5000)) {
            input.getRecord().put("paymentAmount", beneficiaryLimit.add(new BigDecimal(rnd.nextInt(1000000))));
            this.lastBeneficiaryIdTriggered = System.currentTimeMillis();
        }
        if (now - lastPayeeIdBeneficiaryIdTriggered > 12000 + rnd.nextInt(10000)) {
            input.getRecord().put("paymentAmount", payeeBeneficiaryLimit.add(new BigDecimal(rnd.nextInt(1000000))));
            this.lastPayeeIdBeneficiaryIdTriggered = System.currentTimeMillis();
        }
        return input;
    }
}
