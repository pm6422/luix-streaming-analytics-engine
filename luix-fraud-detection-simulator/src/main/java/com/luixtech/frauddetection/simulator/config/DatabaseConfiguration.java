package com.luixtech.frauddetection.simulator.config;

import com.luixtech.frauddetection.simulator.SimulatorApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableJpaRepositories(basePackageClasses = SimulatorApplication.class)
@EnableTransactionManagement
public class DatabaseConfiguration {
}
