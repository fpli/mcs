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

    @Bean
    public DataSource getDataSourceByBridge() {
        logger.info("begin init mysql datasource");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(databaseProperties.getBridgeConfig().getDriver());
        dataSource.setUrl(databaseProperties.getBridgeConfig().getUrl());
        dataSource.setValidationQuery(databaseProperties.getBridgeConfig().getTestStatement());
        logger.info("end init mysql datasource");
        return dataSource;
    }
}
