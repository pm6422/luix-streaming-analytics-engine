package com.luixtech.frauddetection.flinkjob.generator;

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
    public String randomOne(SplittableRandom rnd, long id) {
        T input = wrappedGenerator.randomOne(rnd, id);
        String json;
        try {
            json = objectMapper.writeValueAsString(input);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return json;
    }
}
