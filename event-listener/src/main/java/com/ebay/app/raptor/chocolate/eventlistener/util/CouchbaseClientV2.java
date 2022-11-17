package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.eventlistener.configuration.CacheProperties;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.JacksonTranscoder;
import com.ebay.dukes.nukv.trancoders.StringTranscoder;
import com.ebay.traffic.monitoring.Field;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author yuhxiao
 */
@Component("CouchbaseClientV2")
public class CouchbaseClientV2 {
    /**
     * Global logging instance
     */
    private static final Logger logger = Logger.getLogger(CouchbaseClientV2.class);
    /**
     * Dukes cacheFactory
     */
    @Autowired
    private CacheFactory factory;
    /**
     * Corp couchbase data source
     */
    private String datasourceName;

    private static final String KAFKA_GLOBAL_CONFIG = "KafkaGlobalConfig";

    private static final String SELF_SERVICE_PREFIX = "SelfService_";

    private static final String SELF_SERVICE_METRICS_SUCCESS = "SelfServiceCBSuccess";

    private static final String SELF_SERVICE_METRICS_FAILURE = "SelfServiceCBFailure";

    @Autowired
    CacheProperties cacheProperties;

    @PostConstruct
    private void init() {
        this.datasourceName = cacheProperties.getAppdldevicesdatasource();
    }

    /**
     * Get self-service tracked url
     */
    public String getSelfServiceUrl(String id) {
        String key = SELF_SERVICE_PREFIX + id;
        String url = "";
        CacheClient cacheClient = null;
        try {
            cacheClient = factory.getClient(datasourceName);
            Map m = cacheClient.get(key, new JacksonTranscoder<>(Map.class));
            if (m != null) {
                url = m.get("url").toString();
                logger.info("Get self-service url. id=" + id + " url=" + url);
            }
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception for self-service", e);
            MonitorUtil.info("getCBFail", 1, Field.of("method", "getSelfServiceUrl"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return url;
    }

    /**
     * get kafka global config
     */
    public int getKafkaGlobalConfig() {
        int globalConfig = 0;
        CacheClient cacheClient = null;
        try {
            cacheClient = factory.getClient(datasourceName);
            Map m = cacheClient.get(KAFKA_GLOBAL_CONFIG, new JacksonTranscoder<>(Map.class));
            if (m != null) {
                globalConfig = Integer.parseInt(m.get("globalConfig").toString());
            }
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception", e);
            MonitorUtil.info("getCBFail", 1, Field.of("method", "getKafkaGlobalConfig"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return globalConfig;
    }

    /**
     * Add self-service data
     */
    public void addSelfServiceRecord(String id, String url) {
        try {
            upsertSelfService(id, url);
        } catch (Exception e) {
            MonitorUtil.info(SELF_SERVICE_METRICS_FAILURE);
            logger.warn("Couchbase upsert operation exception for self-service", e);
        }
    }

    /**
     * Couchbase upsert operation for self-service, make sure return client to factory when exception
     */
    private void upsertSelfService(String id, String url) throws Exception {
        CacheClient cacheClient = null;
        try {
            cacheClient = factory.getClient(datasourceName);
            String key = SELF_SERVICE_PREFIX + id;
            if (cacheClient.get(key) == null) {
                Map m = new HashMap<>();
                m.put("url", url);
                cacheClient.set(key, 24 * 60 * 60, m, new JacksonTranscoder<>(Map.class)).get();
                logger.info("Adding new self-service record. id=" + id + " url=" + url);
            }
            MonitorUtil.info(SELF_SERVICE_METRICS_SUCCESS);
        } catch (Exception e) {
            MonitorUtil.info("getCBFail", 1, Field.of("method", "upsertSelfService"));
            throw new Exception(e);
        } finally {
            factory.returnClient(cacheClient);
        }
    }

    /***
     * put key -> val ,default exp 15 days
     * @param key String
     * @param val String
     * @return {@link boolean}
     */
    public boolean put(String key, String val) {
        return put(key, val, 15 * 24 * 60 * 60);
    }

    /**
     * @param key    String
     * @param val    String
     * @param expiry second
     * @return {@link boolean}
     */
    public boolean put(String key, String val, int expiry) {
        CacheClient cacheClient = null;
        boolean result = false;
        try {
            cacheClient = factory.getClient(datasourceName);
            result = cacheClient.set(key, expiry, val, StringTranscoder.getInstance()).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();

            MonitorUtil.info("getCBFail",1, Field.of("method", "put"));

        } finally {
            factory.returnClient(cacheClient);
        }
        return result;
    }

    public String get(String key) {
        CacheClient cacheClient = factory.getClient(datasourceName);
        Object o = cacheClient.get(key, StringTranscoder.getInstance());
        factory.returnClient(cacheClient);
        return o == null ? null : o.toString();
    }
}
