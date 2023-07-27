package com.luixtech.frauddetection.flinkjob.input.param;

import lombok.AllArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
public class ParameterDefinitions {
    // Kafka:
    public static final InputParam<String>  KAFKA_HOST                           = InputParam.string("kafka-host", "localhost");
    public static final InputParam<Integer> KAFKA_PORT                           = InputParam.integer("kafka-port", 9092);
    public static final InputParam<String>  DATA_TOPIC                           = InputParam.string("data-topic", "livetransactions");
    public static final InputParam<String>  ALERTS_TOPIC                         = InputParam.string("alerts-topic", "alerts");
    public static final InputParam<String>  RULES_TOPIC                          = InputParam.string("rules-topic", "rules");
    public static final InputParam<String>  LATENCY_TOPIC                        = InputParam.string("latency-topic", "latency");
    public static final InputParam<String>  RULES_EXPORT_TOPIC                   = InputParam.string("current-rules-topic", "current-rules");
    public static final InputParam<String>  OFFSET                               = InputParam.string("offset", "latest");
    // GCP PubSub:
    public static final InputParam<String>  GCP_PROJECT_NAME                     = InputParam.string("gcp-project", "da-fe-212612");
    public static final InputParam<String>  GCP_PUBSUB_RULES_SUBSCRIPTION        = InputParam.string("pubsub-rules", "rules-demo");
    public static final InputParam<String>  GCP_PUBSUB_ALERTS_SUBSCRIPTION       = InputParam.string("pubsub-alerts", "alerts-demo");
    public static final InputParam<String>  GCP_PUBSUB_LATENCY_SUBSCRIPTION      = InputParam.string("pubsub-latency", "latency-demo");
    public static final InputParam<String>  GCP_PUBSUB_RULES_EXPORT_SUBSCRIPTION = InputParam.string("pubsub-rules-export", "current-rules-demo");
    // Socket
    public static final InputParam<Integer> SOCKET_PORT                          = InputParam.integer("pubsub-rules-export", 9999);
    // General:
    //    source/sink types: kafka / pubsub / socket
    public static final InputParam<String>  RULES_SOURCE                         = InputParam.string("rules-source", "SOCKET");
    public static final InputParam<String>  TRANSACTIONS_SOURCE                  = InputParam.string("data-source", "GENERATOR");
    public static final InputParam<String>  ALERTS_SINK                          = InputParam.string("alerts-sink", "STDOUT");
    public static final InputParam<String>  LATENCY_SINK                         = InputParam.string("latency-sink", "STDOUT");
    public static final InputParam<String>  RULES_EXPORT_SINK                    = InputParam.string("rules-export-sink", "STDOUT");
    public static final InputParam<Integer> RECORDS_PER_SECOND                   = InputParam.integer("records-per-second", 2);
    public static final InputParam<Boolean> LOCAL_WEBSERVER                      = InputParam.bool("local-webserver", false);
    public static final InputParam<Integer> SOURCE_PARALLELISM                   = InputParam.integer("source-parallelism", 2);
    public static final InputParam<Boolean> ENABLE_CHECKPOINTS                   = InputParam.bool("checkpoints", false);
    public static final InputParam<Integer> CHECKPOINT_INTERVAL                  = InputParam.integer("checkpoint-interval", 60_000_0);
    public static final InputParam<Integer> MIN_PAUSE_BETWEEN_CHECKPOINTS        = InputParam.integer("min-pause-btwn-checkpoints", 10000);
    public static final InputParam<Integer> OUT_OF_ORDERNESS                     = InputParam.integer("out-of-orderdness", 500);

    public static final List<InputParam<?>> ALL = Arrays.asList(KAFKA_HOST, DATA_TOPIC, ALERTS_TOPIC, RULES_TOPIC, LATENCY_TOPIC, RULES_EXPORT_TOPIC, OFFSET, GCP_PROJECT_NAME, GCP_PUBSUB_RULES_SUBSCRIPTION, GCP_PUBSUB_ALERTS_SUBSCRIPTION, GCP_PUBSUB_LATENCY_SUBSCRIPTION, GCP_PUBSUB_RULES_EXPORT_SUBSCRIPTION, RULES_SOURCE, TRANSACTIONS_SOURCE, ALERTS_SINK, LATENCY_SINK, RULES_EXPORT_SINK,
            KAFKA_PORT, SOCKET_PORT, RECORDS_PER_SECOND, SOURCE_PARALLELISM, CHECKPOINT_INTERVAL, MIN_PAUSE_BETWEEN_CHECKPOINTS, OUT_OF_ORDERNESS,
            LOCAL_WEBSERVER, ENABLE_CHECKPOINTS);

    private final ParameterTool parameterTool;

    <T> T getOrDefaultValue(InputParam<T> inputParam) {
        if (!parameterTool.has(inputParam.getName())) {
            return inputParam.getDefaultValue();
        }
        Object value;
        if (inputParam.getType() == Integer.class) {
            value = parameterTool.getInt(inputParam.getName());
        } else if (inputParam.getType() == Long.class) {
            value = parameterTool.getLong(inputParam.getName());
        } else if (inputParam.getType() == Double.class) {
            value = parameterTool.getDouble(inputParam.getName());
        } else if (inputParam.getType() == Boolean.class) {
            value = parameterTool.getBoolean(inputParam.getName());
        } else {
            value = parameterTool.get(inputParam.getName());
        }
        return inputParam.getType().cast(value);
    }

    public static ParameterDefinitions fromArgs(String[] args) {
        ParameterTool tool = ParameterTool.fromArgs(args);
        return new ParameterDefinitions(tool);
    }
}
