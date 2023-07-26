package com.luixtech.frauddetection.flinkjob.generator;

import com.luixtech.frauddetection.flinkjob.generator.BaseGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.SplittableRandom;

public class JsonGeneratorWrapper<T> extends BaseGenerator<String> {

    private final        BaseGenerator<T> wrappedGenerator;
    private static final ObjectMapper     objectMapper = new ObjectMapper();

    public JsonGeneratorWrapper(BaseGenerator<T> wrappedGenerator) {
        this.wrappedGenerator = wrappedGenerator;
        this.maxRecordsPerSecond = wrappedGenerator.getMaxRecordsPerSecond();
    }

    @Override
    public String randomEvent(SplittableRandom rnd, long id) {
        T transaction = wrappedGenerator.randomEvent(rnd, id);
        String json;
        try {
            json = objectMapper.writeValueAsString(transaction);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return json;
    }
}
