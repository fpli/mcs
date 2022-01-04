package com.ebay.app.raptor.chocolate.filter.configs;

import com.ebay.dukes.component.api.BaseCacheFactoryConfiguration;
import com.ebay.dukes.component.api.CacheFactoryConfiguration;
import com.ebay.dukes.component.api.DukesCacheConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author yuhxiao
 */
@Configuration
public class CacheConfig {
    @Autowired
    CacheProperties cacheProperties;

    @Bean
    @DukesCacheConfig(factoryName = "chocofilCacheFactory")
    public CacheFactoryConfiguration getCacheConfig() {
        return BaseCacheFactoryConfiguration.newBuilder()
                .forCaches(cacheProperties.getDatasource())
                .validating(false)
                .build();
    }
}
