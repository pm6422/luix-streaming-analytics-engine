package com.ververica.demo.backend;

import com.luixtech.framework.EnableLuixFramework;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
@SpringBootApplication
@EnableLuixFramework
@ComponentScan("com.ververica")
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
