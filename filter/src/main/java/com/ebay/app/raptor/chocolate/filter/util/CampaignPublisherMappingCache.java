package com.ebay.app.raptor.chocolate.filter.util;

import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple campaign ID-to-publisher cache. Basically what this does is look up a publisher ID for a given campaign ID.
 * This is a thread-safe class by design.
 *
 * @author Lu huichun
 */
public class CampaignPublisherMappingCache {
    /** Logging instance. */
    private static final Logger logger = Logger.getLogger(CampaignPublisherMappingCache.class);
    /** Static instance */
    private volatile static CampaignPublisherMappingCache INSTANCE = null;
    /** CampaignId - PublisherId cache */
    final ConcurrentHashMap<Long, Long> cache = new ConcurrentHashMap<>();

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
    public Long lookup(long campaignId) {
        Long publisherId = cache.get(campaignId);
        if (publisherId == null) {
            publisherId = CouchbaseClient.getInstance().getPublisherID(campaignId);
            cache.putIfAbsent(campaignId, publisherId);
        }
        return publisherId;
    }

    /**
     * Enters a mapping into the campaign -> publisher cache. Meant to be called from the Zookeeper entry point. This is
     * synchronized because we check for uniqueness in the init method, and if we don't have a synchronized here, then,
     * technically, there's an edge case where we could unnecessarily throw an exception by inserting duplicate keys via
     * zookeeper.
     *
     * @param campaignId to create new mapping for
     * @param publisherId to create new mapping for
     */
    public synchronized void addMapping(long campaignId, long publisherId) {
        logger.debug("Adding new mapping. campaignId=" + campaignId + " publisherId=" + publisherId);
        cache.putIfAbsent(campaignId, publisherId);
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
