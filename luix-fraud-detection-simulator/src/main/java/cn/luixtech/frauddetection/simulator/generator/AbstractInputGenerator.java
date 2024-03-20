package cn.luixtech.frauddetection.simulator.generator;

import cn.luixtech.dae.common.input.Input;
import com.luixtech.utilities.thread.Throttler;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

@Slf4j
public abstract class AbstractInputGenerator implements Runnable {

    private static final String          PAYMENT_TYPE_CSH   = "CSH";
    private static final String          PAYMENT_TYPE_CRD   = "CRD";
    private static final long            MAX_PAYEE_ID       = 100000;
    private static final long            MAX_BENEFICIARY_ID = 100000;
    private static final double          MIN_PAYMENT_AMOUNT = 5d;
    private static final double          MAX_PAYMENT_AMOUNT = 20d;
    private final        Throttler       throttler;
    private volatile     boolean         running            = true;
    private final        Integer         maxRecordsPerSecond;
    private final        Consumer<Input> inputProducer;

    public AbstractInputGenerator(Consumer<Input> inputProducer, int maxRecordsPerSecond) {
        this.inputProducer = inputProducer;
        this.maxRecordsPerSecond = maxRecordsPerSecond;
        this.throttler = new Throttler(maxRecordsPerSecond, 1);
    }

    public void adjustMaxRecordsPerSecond(long maxRecordsPerSecond) {
        throttler.adjustMaxRecordsPerSecond(maxRecordsPerSecond);
    }

    protected Input randomOne(SplittableRandom rnd, Long eventTime) {
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
                .tenant("tesla")
                .groupingValues(Map.of("tenant", "tesla"))
                .build();
    }

    public Input generateOne(long now) {
        return randomOne(new SplittableRandom(), now);
    }

    public void generateAndPublishOne(long now) {
        inputProducer.accept(generateOne(now));
    }

    private static String paymentType(long id) {
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

    @Override
    public final void run() {
        running = true;

        final SplittableRandom rnd = new SplittableRandom();

        while (running) {
            try {
                Input input = randomOne(rnd, null);
                inputProducer.accept(input);
                throttler.throttle();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        log.info("Finished run()");
    }

    public final void cancel() {
        running = false;
        log.info("Cancelled");
    }
}
