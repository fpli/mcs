package com.ebay.app.raptor.chocolate.eventlistener.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author yuhxiao
 */
@ConfigurationProperties(prefix = "nukv")
public class CacheProperties {
    private String appdldevicesdatasource;

    public String getAppdldevicesdatasource() {
        return appdldevicesdatasource;
    }

    public void setAppdldevicesdatasource(String appdldevicesdatasource) {
        this.appdldevicesdatasource = appdldevicesdatasource;
    }
}
