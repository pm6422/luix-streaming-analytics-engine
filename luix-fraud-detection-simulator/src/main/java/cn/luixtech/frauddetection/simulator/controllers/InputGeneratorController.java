package cn.luixtech.frauddetection.simulator.controllers;

import cn.luixtech.frauddetection.simulator.config.ApplicationProperties;
import cn.luixtech.frauddetection.simulator.generator.InputGenerator;
import cn.luixtech.frauddetection.simulator.kafka.consumer.KafkaInputConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

@RestController
@Slf4j
public class InputGeneratorController {

    private static final ExecutorService       EXECUTOR_SERVICE         = Executors.newSingleThreadExecutor();
    @Resource
    private              InputGenerator        inputGenerator;
    @Resource
    private              ApplicationProperties applicationProperties;
    @Resource
    private              KafkaInputConsumer    kafkaInputConsumer;
    private              boolean               listenerContainerRunning = true;
    private static final AtomicBoolean         GENERATING               = new AtomicBoolean(false);

    @GetMapping("/api/input-generator/start")
    public void start(@RequestParam(value = "quantity", required = false) Integer quantity) {
        if (quantity != null) {
            long now = System.currentTimeMillis();
            IntStream.range(0, quantity).forEach(i -> inputGenerator.generateAndPublishOne(now));
            return;
        }
        if (GENERATING.compareAndSet(false, true)) {
            EXECUTOR_SERVICE.submit(inputGenerator);
        }
    }

    @GetMapping("/api/input-generator/stop")
    public void stop() {
        if (GENERATING.compareAndSet(true, false)) {
            inputGenerator.cancel();
        }
    }

    @GetMapping("/api/input-generator/speed/{speed}")
    public void setGeneratorSpeed(@PathVariable Long speed) {
        log.info("Generator speed change request: " + speed);
        if (speed <= 0) {
            inputGenerator.cancel();
            GENERATING.set(false);
            return;
        } else {
            start(null);
        }

        if (speed > applicationProperties.getInput().getMaxInputSpeed()) {
            kafkaInputConsumer.stop();
            listenerContainerRunning = false;
        } else if (!listenerContainerRunning) {
            kafkaInputConsumer.start();
        }

        if (inputGenerator != null) {
            inputGenerator.adjustMaxRecordsPerSecond(speed);
        }
    }
}
