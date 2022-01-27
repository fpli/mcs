package com.ebay.app.raptor.chocolate.filter.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "nukv")
public class CacheProperties {
    private String datasource;
    private String dbEnv;
    private String dnsRegion;
    private String poolType;

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getDbEnv() {
        return dbEnv;
    }

    public void setDbEnv(String dbEnv) {
        this.dbEnv = dbEnv;
    }

    public String getDnsRegion() {
        return dnsRegion;
    }

    public void setDnsRegion(String dnsRegion) {
        this.dnsRegion = dnsRegion;
    }

    public String getPoolType() {
        return poolType;
    }

    public void setPoolType(String poolType) {
        this.poolType = poolType;
    }
}
