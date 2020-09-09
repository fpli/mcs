package com.ebay.app.raptor.chocolate.eventlistener.configuration;

import com.ebay.app.raptor.chocolate.eventlistener.component.FideliusClient;
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
        fideliusClient.create("mktcollectionsvc.mysqlpassword", pass);
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(databaseProperties.getBridgeConfig().getUrl());
        dataSource.setUsername(userName);
        dataSource.setPassword(pass);
//        dataSource.setPassword(fideliusClient.getContent(pass));
        logger.info("channel name config {} / pass config {} password {}", userName, pass, fideliusClient.getContent("mktcollectionsvc.mysqlpassword"));
        dataSource.setValidationQuery(databaseProperties.getBridgeConfig().getTestStatement());
        return dataSource;
    }
}
