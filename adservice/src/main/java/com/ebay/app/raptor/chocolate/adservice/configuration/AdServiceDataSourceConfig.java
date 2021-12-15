package com.ebay.app.raptor.chocolate.adservice.configuration;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @Auther YangYang
 */
@Configuration
public class AdServiceDataSourceConfig {
    Logger logger = LoggerFactory.getLogger(AdServiceDataSourceConfig.class);

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
