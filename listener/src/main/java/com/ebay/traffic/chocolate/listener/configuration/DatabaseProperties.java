package com.ebay.traffic.chocolate.listener.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "database")
public class DatabaseProperties {

    private BridgeConfig bridgeConfig;

    public BridgeConfig getBridgeConfig() {
        return bridgeConfig;
    }

    public void setBridgeConfig(BridgeConfig bridgeConfig) {
        this.bridgeConfig = bridgeConfig;
    }

    public static class BridgeConfig {

        private String usernameChannel;
        private String passwordChannel;
        private String url;
        private String driver;
        private String testStatement;

        private Boolean removeAbandoned;
        private Integer removeAbandonedTimeout;
        private Boolean logAbandoned;

        private Long timeBetweenEvictionRunsMillis;
        private Long minEvictableIdleTimeMillis;
        private Boolean testWhileIdle;

        public String getUsernameChannel() {
            return usernameChannel;
        }

        public void setUsernameChannel(String usernameChannel) {
            this.usernameChannel = usernameChannel;
        }

        public String getPasswordChannel() {
            return passwordChannel;
        }

        public void setPasswordChannel(String passwordChannel) {
            this.passwordChannel = passwordChannel;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getTestStatement() {
            return testStatement;
        }

        public void setTestStatement(String testStatement) {
            this.testStatement = testStatement;
        }

        public Boolean getRemoveAbandoned() {
            return removeAbandoned;
        }

        public void setRemoveAbandoned(Boolean removeAbandoned) {
            this.removeAbandoned = removeAbandoned;
        }

        public Integer getRemoveAbandonedTimeout() {
            return removeAbandonedTimeout;
        }

        public void setRemoveAbandonedTimeout(Integer removeAbandonedTimeout) {
            this.removeAbandonedTimeout = removeAbandonedTimeout;
        }

        public Boolean getLogAbandoned() {
            return logAbandoned;
        }

        public void setLogAbandoned(Boolean logAbandoned) {
            this.logAbandoned = logAbandoned;
        }

        public Long getTimeBetweenEvictionRunsMillis() {
            return timeBetweenEvictionRunsMillis;
        }

        public void setTimeBetweenEvictionRunsMillis(Long timeBetweenEvictionRunsMillis) {
            this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        }

        public Long getMinEvictableIdleTimeMillis() {
            return minEvictableIdleTimeMillis;
        }

        public void setMinEvictableIdleTimeMillis(Long minEvictableIdleTimeMillis) {
            this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
        }

        public Boolean getTestWhileIdle() {
            return testWhileIdle;
        }

        public void setTestWhileIdle(Boolean testWhileIdle) {
            this.testWhileIdle = testWhileIdle;
        }
    }
}
