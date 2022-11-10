package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.filter.configs.CacheProperties;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.nukv.trancoders.StringTranscoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author yuhxiao
 */
@Component(value = "CouchbaseClientV2")
public class CouchbaseClientV2 {
    /**
     * Global logging instance
     */
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseClientV2.class);
    /**
     * default publisherID
     */
    private static final long DEFAULT_PUBLISHER_ID = -1L;
    /**
     * flush buffer to keep record when couchbase down
     */
    private Queue<Map.Entry<Long, Long>> buffer = new LinkedBlockingDeque<>();

    @Autowired
    private CacheFactory factory;

    @Autowired
    CacheProperties cacheProperties;

    /**
     * add mapping pair into couchbase
     */
    public void addMappingRecord(long campaignId, long publisherId) {
        flushBuffer();
        try {
            upsert(campaignId, publisherId);
        } catch (Exception e) {
            buffer.add(new AbstractMap.SimpleEntry<>(campaignId, publisherId));
            logger.warn("Couchbase upsert operation exception", e);
        }
    }

    private void upsert(long campaignId, long publisherId) throws Exception {
        CacheClient cacheClient = null;
        try {
            cacheClient = factory.getClient(cacheProperties.getDatasource());
            if (cacheClient.get(String.valueOf(campaignId), StringTranscoder.getInstance()) == null) {
                cacheClient.set(String.valueOf(campaignId), 2500, String.valueOf(publisherId), StringTranscoder.getInstance());
                logger.debug("Adding new mapping. campaignId=" + campaignId + " publisherId=" + publisherId);
            }
        } catch (Exception e) {
            MonitorUtil.info("CBDeSetException",1);
            MonitorUtil.info("CBDeGetException",1);
            throw new Exception(e);
        } finally {
            factory.returnClient(cacheClient);
        }
    }

    /**
     * This method will upsert the records in buffer when couchbase recovery
     */
    private void flushBuffer() {
        try {
            while (!buffer.isEmpty()) {
                Map.Entry<Long, Long> kv = buffer.peek();
                upsert(kv.getKey(), kv.getValue());
                buffer.poll();
            }
        } catch (Exception e) {
            logger.warn("Couchbase upsert operation exception", e);
        }
    }

    /**
     * Get publisherId by campaignId
     */
    public long getPublisherID(long campaignId) throws InterruptedException {
        MonitorUtil.info("FilterCouchbaseQuery");
        CacheClient cacheClient = null;
        int retry = 0;
        while (retry < 3) {
            try {
                long start = System.currentTimeMillis();
                cacheClient = factory.getClient(cacheProperties.getDatasource());
                Object o = cacheClient.get(String.valueOf(campaignId), StringTranscoder.getInstance());
                MonitorUtil.latency("FilterCouchbaseLatency", System.currentTimeMillis() - start);
                if (o == null) {
                    logger.warn("No publisherID found for campaign " + campaignId + " in couchbase");
                    MonitorUtil.info("ErrorPublishID");
                    return DEFAULT_PUBLISHER_ID;
                }
                MonitorUtil.info("GetNukvSuccess",1);
                return Long.parseLong(o.toString());
            } catch (NumberFormatException ne) {
                logger.warn("Error in converting publishID " + factory.getClient(cacheProperties.getDatasource()).get(String.valueOf(campaignId),
                        StringTranscoder.getInstance()).toString() + " to Long", ne);
                MonitorUtil.info("ErrorPublishID");
                return DEFAULT_PUBLISHER_ID;
            } catch (Exception e) {
                MonitorUtil.info("FilterCouchbaseRetry");
                logger.warn("Couchbase query operation timeout, will sleep for 1s to retry", e);
                Thread.sleep(1000);
                ++retry;
            } finally {
                factory.returnClient(cacheClient);
            }
        }
        return DEFAULT_PUBLISHER_ID;
    }

    public void deleteMappingRecord(long campaignId) {
        CacheClient client = factory.getClient(cacheProperties.getDatasource());
        try {
            Boolean result = client.delete(String.valueOf(campaignId)).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
