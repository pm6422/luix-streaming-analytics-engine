package com.ververica.demo.backend.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.ververica.demo.backend.WebCenterLauncher;

@Configuration
@EnableJpaRepositories(basePackageClasses = WebCenterLauncher.class)
@EnableTransactionManagement
public class DatabaseConfiguration {
}
