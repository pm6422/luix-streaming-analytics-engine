package com.ververica.demo.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.ververica")
public class WebCenterLauncher {
  public static void main(String[] args) {
    SpringApplication.run(WebCenterLauncher.class, args);
  }
}
