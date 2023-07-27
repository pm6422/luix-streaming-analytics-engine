package com.luixtech.frauddetection.simulator.config;

import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

/**
 * Properties specific to Application.
 *
 * <p>
 * Properties are configured in the application.yml file.
 * </p>
 */
@Component
@ConfigurationProperties(prefix = "application", ignoreUnknownFields = false)
@Validated
@Getter
public class ApplicationProperties {

    private final WebSocket   webSocket   = new WebSocket();
    private final Transaction transaction = new Transaction();

    @Data
    public static class WebSocket {
        private Topic topic;

        @Data
        public static class Topic {
            private String transactions;
            private String alerts;
            private String latency;
        }
    }

    @Data
    public static class Transaction {
        private long maxTransactionSpeed;
    }

}
