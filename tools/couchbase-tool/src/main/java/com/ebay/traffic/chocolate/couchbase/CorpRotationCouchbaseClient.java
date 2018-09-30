package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;

import java.util.Properties;

public class CorpRotationCouchbaseClient {
    private CacheFactory factory;
    private Properties properties;

    public CorpRotationCouchbaseClient(Properties properties) {
        this.properties = properties;
        init();
    }

    private void init() {
        factory =
          com.ebay.dukes.builder.GenericCacheFactoryBuilder.newBuilder()
            .cache(properties.getProperty("couchbase.corp.rotation.dataSource"))
            .identityFileDirectoryLocation(properties.getProperty("couchbase.corp.rotation.identitiesLoc"))
            .dbEnv(properties.getProperty("couchbase.corp.rotation.dbEnv"))
            .deploymentSlot(properties.getProperty("couchbase.corp.rotation.deploymentSlot"))
            .dnsRegion(properties.getProperty("couchbase.corp.rotation.dnsRegion"))
            .pool(properties.getProperty("couchbase.corp.rotation.pool"))
            .poolType(properties.getProperty("couchbase.corp.rotation.poolType"))
            .appName(properties.getProperty("couchbase.corp.rotation.appName"))
            .build();
    }

    public CacheClient getCacheClient() {
        return factory.getClient(properties.getProperty("couchbase.corp.rotation.dataSource"));
    }

    public Bucket getBuctet(CacheClient cacheClient) {
        if (cacheClient == null) {
            return null;
        }
        BaseDelegatingCacheClient baseDelegatingCacheClient = (BaseDelegatingCacheClient) cacheClient;
        Couchbase2CacheClient couchbase2CacheClient = (Couchbase2CacheClient) baseDelegatingCacheClient.getCacheClient();
        return couchbase2CacheClient.getCouchbaseClient();
    }

    public void returnClient(CacheClient cacheClient) {
        if (cacheClient != null) {
            factory.returnClient(cacheClient);
        }
    }

    public void shutdown() {
        if (factory != null) {
            factory.shutdown();
        }
    }
}
