package com.luixtech.frauddetection.flinkjob.output;

import com.luixtech.frauddetection.common.dto.Alert;
import com.luixtech.frauddetection.common.dto.Transaction;
import com.luixtech.frauddetection.flinkjob.core.Arguments;
import com.luixtech.frauddetection.flinkjob.output.sinkcreator.SinkCreator;
import com.luixtech.frauddetection.flinkjob.serializer.JsonSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.math.BigDecimal;

public class AlertSink {

    public static DataStream<String> alertsStreamToJson(DataStream<Alert> alerts) {
        return alerts.flatMap(new JsonSerializer<>(Alert.class)).name("Alerts Serialization");
    }

    public static DataStreamSink<String> addAlertsSink(Arguments arguments, DataStream<String> stream) {
        return SinkCreator
                .getInstance("alert-" + arguments.messageChannel)
                .create(stream, arguments);
    }
}
