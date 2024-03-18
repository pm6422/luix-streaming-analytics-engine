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

    private final Input input = new Input();
    private final Kafka kafka = new Kafka();

    @Data
    public static class Input {
        private long maxInputSpeed;
    }

    @Data
    public static class Kafka {
        private Topic    topic;
        private Listener listener;

        @Data
        public static class Topic {
            private String input;
            private String alert;
            private String latency;
            private String rule;
        }

        @Data
        public static class Listener {
            private String input;
        }
    }
}
