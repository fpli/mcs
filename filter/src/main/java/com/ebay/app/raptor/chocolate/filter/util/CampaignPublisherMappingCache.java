package com.ebay.app.raptor.chocolate.filter.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple campaign ID-to-publisher cache. Basically what this does is look up a publisher ID for a given campaign ID.
 * This is a thread-safe class by design.
 *
 * @author Lu huichun
 */
public class CampaignPublisherMappingCache {
    /** Logging instance. */
    private static final Logger logger = LoggerFactory.getLogger(CampaignPublisherMappingCache.class);
    /** Static instance */
    private volatile static CampaignPublisherMappingCache INSTANCE = null;
    /** CampaignId - PublisherId cache */
    final ConcurrentHashMap<Long, Long> cache = new ConcurrentHashMap<>();

    private static final long DEFAULT_PUBLISHER_ID = -1L;


    @Override
    public String toString() {
        return "cache size= " + cache.size();
    }

    /**
     *Singleton
     */
    private CampaignPublisherMappingCache() {}

    /**
     * Initialize publisher cache
     */
    private static void init() {
        if (INSTANCE == null) {
            logger.info("Creating new campaign-to-publisher cache instance");
            INSTANCE= new CampaignPublisherMappingCache();
        } else {
            logger.warn("Re-initializing cache. Existing cache size=" + INSTANCE.cache.size());
            INSTANCE.cache.clear();
        }
    }

    /**
     * Looks up a particular publisher for a given campaign ID.
     * @param campaignId the campaignId key
     * @return null if there is no such campaignId
     */
    public Long lookup(long campaignId) throws InterruptedException {
        Long publisherId = cache.get(campaignId);
        if (publisherId == null) {
            CouchbaseClientV2 couchbaseClientV2 = SpringUtils.getBean("CouchbaseClientV2", CouchbaseClientV2.class);
            publisherId = couchbaseClientV2.getPublisherID(campaignId);
            if (publisherId != DEFAULT_PUBLISHER_ID)
                cache.putIfAbsent(campaignId, publisherId);
        }
        logger.debug("Get publisherID " + publisherId + " for campaignID " + campaignId);
        return publisherId;
    }

    /**
     * Destroy the PublisherCache
     */
    public static synchronized void destroy() {
        if (INSTANCE != null){
            INSTANCE.cache.clear();
            INSTANCE = null;
        }
    }

    /**
     * @return the instance
     */
    public static CampaignPublisherMappingCache getInstance() {
        if (INSTANCE == null) {
            synchronized (CampaignPublisherMappingCache.class) {
                if (INSTANCE == null)
                    init();
            }
        }
        return INSTANCE;
    }

}
