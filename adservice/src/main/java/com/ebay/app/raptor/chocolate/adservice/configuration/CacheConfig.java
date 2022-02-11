package com.ebay.app.raptor.chocolate.adservice.configuration;

import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.builder.DefaultCacheFactoryBuilder;
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
        return DefaultCacheFactoryBuilder.newBuilder().cache(cacheProperties.getDatasource()).build();
    }
}
