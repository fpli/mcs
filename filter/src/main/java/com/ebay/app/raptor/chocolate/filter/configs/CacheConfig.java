package com.ebay.app.raptor.chocolate.filter.configs;

import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.builder.FountCacheFactoryBuilder;
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
    public CacheFactory cacheFactory() {
        return FountCacheFactoryBuilder.newBuilder()
                .appName("chocofil")
                .cache(cacheProperties.getDatasource())
                .dbEnv(cacheProperties.getDbEnv())
                .dnsRegion(cacheProperties.getDnsRegion())
                .poolType(cacheProperties.getPoolType())
                .forceClearTextPasswords(true)
                .build();
    }
}
