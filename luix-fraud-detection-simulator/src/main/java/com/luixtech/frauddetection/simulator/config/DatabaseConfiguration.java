package com.luixtech.frauddetection.simulator.config;

import com.luixtech.frauddetection.simulator.SimulatorLauncher;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableJpaRepositories(basePackageClasses = SimulatorLauncher.class)
@EnableTransactionManagement
public class DatabaseConfiguration {
}
