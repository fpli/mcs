package com.ebay.traffic.chocolate.listener.configuration;

import com.ebay.traffic.chocolate.listener.component.FideliusClient;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @Author YangYang
 */
@Configuration
public class DataSourceConfig {
    Logger logger = LoggerFactory.getLogger(DataSourceConfig.class);

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
        dataSource.setPassword(fideliusClient.getContent("chocolis.mysqlpassword"));
        dataSource.setValidationQuery(databaseProperties.getBridgeConfig().getTestStatement());
        return dataSource;
    }
}
