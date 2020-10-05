package com.ebay.app.raptor.chocolate.adservice.configuration;

import com.ebay.app.raptor.chocolate.adservice.component.FideliusClient;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.HashMap;

/**
 * @Auther YangYang
 */
@Configuration
public class AdServiceDataSourceConfig {
    Logger logger = LoggerFactory.getLogger(AdServiceDataSourceConfig.class);

    @Autowired
    private DatabaseProperties databaseProperties;

    @Autowired
    FideliusClient fideliusClient;

    @Bean
    public DataSource getDataSourceByBridge() {
        logger.info("databaseProperties: {}", databaseProperties);
        String userName = databaseProperties.getBridgeConfig().getUsernameChannel();
        String pass = databaseProperties.getBridgeConfig().getPasswordChannel();
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(databaseProperties.getBridgeConfig().getUrl());
        dataSource.setUsername(userName);
        logger.info("channel name config {} / pass config {}", userName, pass);
        dataSource.setPassword(fideliusClient.getContent(pass));
        dataSource.setValidationQuery(databaseProperties.getBridgeConfig().getTestStatement());
        return dataSource;
    }
}
