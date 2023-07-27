package com.luixtech.frauddetection.simulator;

import com.luixtech.framework.EnableLuixFramework;
import com.luixtech.frauddetection.simulator.config.ApplicationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({ApplicationProperties.class})
@EnableLuixFramework
public class SimulatorLauncher {
    /**
     * Entrance method which used to run the application. Spring profiles can be configured with a program arguments
     * --spring.profiles.active=your-active-profile
     *
     * @param args program arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(SimulatorLauncher.class, args);
    }
}
