package com.student.springbatchproject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import org.springframework.context.annotation.Primary;
import org.springframework.beans.factory.annotation.Qualifier;

@Configuration
public class MultipleDataSourceConfig {

    @Value("${app.target-db}")
    private String targetDb;

    private final Environment env;

    public MultipleDataSourceConfig(Environment env) {
        this.env = env;
    }

    @Bean
    @Primary
    public DataSource dataSource() {
        String url = env.getProperty("spring.datasource.batch.url");
        String username = env.getProperty("spring.datasource.batch.username");
        String password = env.getProperty("spring.datasource.batch.password");
        String driver = env.getProperty("spring.datasource.batch.driver-class-name");

        if (url == null || username == null || driver == null) {
            throw new IllegalStateException("Missing batch datasource configuration (spring.datasource.batch.*)");
        }

        return DataSourceBuilder.create()
                .url(url)
                .username(username)
                .password(password)
                .driverClassName(driver)
                .build();
    }


    @Bean
    @Qualifier("targetDataSource")
    public DataSource targetDataSource() {
        String keyPrefix;
        switch (targetDb.toLowerCase()) {
            case "mysql":
                keyPrefix = "spring.datasource.mysql";
                break;
            case "oracle":
                keyPrefix = "spring.datasource.oracle";
                break;
            case "postgres":
            default:
                keyPrefix = "spring.datasource.postgres";
                break;
        }

        String url = env.getProperty(keyPrefix + ".url");
        String username = env.getProperty(keyPrefix + ".username");
        String password = env.getProperty(keyPrefix + ".password");
        String driver = env.getProperty(keyPrefix + ".driver-class-name");

        if (url == null || username == null || driver == null) {
            throw new IllegalStateException("Missing target datasource configuration for: " + targetDb);
        }

        return DataSourceBuilder.create()
                .url(url)
                .username(username)
                .password(password)
                .driverClassName(driver)
                .build();
    }


    @Bean
    public JdbcTemplate targetJdbcTemplate(@Qualifier("targetDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }
}
