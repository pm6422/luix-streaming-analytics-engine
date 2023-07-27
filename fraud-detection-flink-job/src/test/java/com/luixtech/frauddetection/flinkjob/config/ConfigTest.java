package com.luixtech.frauddetection.flinkjob.config;

import static org.junit.Assert.assertEquals;

import com.luixtech.frauddetection.flinkjob.input.param.Parameters;
import com.luixtech.frauddetection.flinkjob.input.param.ParameterDefinitions;
import org.junit.Test;

public class ConfigTest {

  @Test
  public void testParameters() {
    String[] args = new String[] {"--kafka-host", "host-from-args"};
    ParameterDefinitions parameterDefinitions = ParameterDefinitions.fromArgs(args);
    Parameters parameters = Parameters.fromDefinitions(parameterDefinitions);

    final String kafkaHost = parameters.getValue(ParameterDefinitions.KAFKA_HOST);
    assertEquals("Wrong config parameter retrieved", "host-from-args", kafkaHost);
  }

  @Test
  public void testParameterWithDefaults() {
    String[] args = new String[] {};
    ParameterDefinitions parameterDefinitions = ParameterDefinitions.fromArgs(args);
    Parameters parameters = Parameters.fromDefinitions(parameterDefinitions);

    final Integer kafkaPort = parameters.getValue(ParameterDefinitions.KAFKA_PORT);
    assertEquals("Wrong config parameter retrieved", Integer.valueOf(9092), kafkaPort);
  }
}
