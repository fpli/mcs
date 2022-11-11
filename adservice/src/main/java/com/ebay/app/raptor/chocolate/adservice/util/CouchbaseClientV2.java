package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.configuration.CacheProperties;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.JacksonTranscoder;
import com.ebay.dukes.nukv.trancoders.StringTranscoder;
import com.ebay.traffic.monitoring.Field;
import com.mysql.jdbc.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author xiangli4
 * @since 2019/12/11
 */
@Component
public class CouchbaseClientV2 {
    /**
     * Global logging instance
     */
    private static final Logger logger = Logger.getLogger(CouchbaseClientV2.class);

    @Autowired
    private CacheFactory factory;

    /**
     * Corp couchbase data source
     */
    private String datasourceName;

    @Autowired
    CacheProperties cacheProperties;

    /**
     * TTL is larger than 30 days, expiry is the end timestamp
     */
    private static final int EXPIRY = (int) (System.currentTimeMillis() / 1000) + 31 * 24 * 60 * 60;

    private static final String GUID_MAP_KEY = "guid";
    private static final String UID_MAP_KEY = "uid";
    private static final String ADGUID_MAP_KEY = "adguid";

    @PostConstruct
    public void init() {
        this.datasourceName = cacheProperties.getDatasource();
    }

    /**
     * add mapping pair into couchbase
     *
     * @param adguid   adguid
     * @param guidList new guid list
     * @param guid     new guid
     * @return upserted
     */
    public boolean addMappingRecord(String adguid, String guidList, String guid, String uid) {
        boolean upserted = false;
        try {
            upserted = upsert(adguid, guidList, guid, uid);
        } catch (Exception e) {
            logger.warn("Couchbase upsert operation exception", e);
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","addMappingRecord"));
        }
        return upserted;
    }

    /*get guid list by adguid*/
    public String getGuidListByAdguid(String adguid) {
        CacheClient cacheClient = null;
        String guidList = "";
        try {
            cacheClient = factory.getClient(datasourceName);
            Map map = cacheClient.get(IdMapable.ADGUID_GUID_PREFIX + adguid, new JacksonTranscoder<>(Map.class));
            if (map != null) {
                guidList = map.get(GUID_MAP_KEY).toString();
                logger.debug("Get guid list. adguid=" + adguid + " guidList=" + guidList);
            }
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception", e);
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","getGuidListByAdguid"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return guidList;
    }

    /*get uid by adguid*/
    public String getUidByAdguid(String adguid) {
        CacheClient cacheClient = null;
        String uid = "";
        try {
            cacheClient = factory.getClient(datasourceName);
            Map map = cacheClient.get(IdMapable.ADGUID_UID_PREFIX + adguid, new JacksonTranscoder<>(Map.class));
            if (map != null) {
                uid = map.get(UID_MAP_KEY).toString();
                logger.debug("Get user id. adguid=" + adguid + " uid=" + uid);
            }
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception", e);
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","getUidByAdguid"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return uid;
    }

    /*get adguid by guid*/
    public String getAdguidByGuid(String guid) {
        CacheClient cacheClient = null;
        String adguid = "";
        try {
            cacheClient = factory.getClient(datasourceName);
            Map map = cacheClient.get(IdMapable.GUID_ADGUID_PREFIX + guid, new JacksonTranscoder<>(Map.class));
            if (map != null) {
                adguid = map.get(ADGUID_MAP_KEY).toString();
                logger.debug("Get adguid. guid=" + guid + " adguid=" + adguid);
            }
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception", e);
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","getAdguidByGuid"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return adguid;
    }

    /*get uid by guid*/
    public String getUidByGuid(String guid) {
        CacheClient cacheClient = null;
        String uid = "";
        try {
            cacheClient = factory.getClient(datasourceName);
            Map map = cacheClient.get(IdMapable.GUID_UID_PREFIX + guid, new JacksonTranscoder<>(Map.class));
            if (map != null) {
                uid = map.get(UID_MAP_KEY).toString();
                MonitorUtil.info("getNukvSuccess");
                logger.debug("Get user id. guid=" + guid + " uid=" + uid);
            }
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception", e);
            MonitorUtil.info("getNukvFail");
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","getUidByGuid"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return uid;
    }

    /*get guid by uid*/
    public String getGuidByUid(String uid) {
        CacheClient cacheClient = null;
        String guid = "";
        try {
            cacheClient = factory.getClient(datasourceName);
            Map map = cacheClient.get(IdMapable.UID_GUID_PREFIX + uid, new JacksonTranscoder<>(Map.class));
            if (map != null) {
                guid = map.get(GUID_MAP_KEY).toString();
                logger.debug("Get guid. uid=" + uid + " guid=" + guid);
            }
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception", e);
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","getGuidByUid"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return guid;
    }

    private void addSingleMapping(String idKey, String idValue, String mapPrefix, String mapName) {
        CacheClient cacheClient = factory.getClient(datasourceName);
        String key = mapPrefix + idKey;
        if (!StringUtils.isNullOrEmpty(idKey) && !StringUtils.isNullOrEmpty(idValue)) {
            Map<String, String> map = new HashMap<>();
            map.put(mapName, idValue);
            try {
                cacheClient.set(key, EXPIRY, map, new JacksonTranscoder<>(Map.class)).get();
                MonitorUtil.info("addNukvSuccess");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                MonitorUtil.info("addCBDeFaile", 1, Field.of("method","addSingleMapping"));
            } finally {
                factory.returnClient(cacheClient);
            }
            logger.debug("Adding new mapping. Map type:" + mapPrefix + ", key: " + idKey + ", value: " + idValue);
        }
    }

    /**
     * Couchbase upsert operation, make sure return client to factory when exception
     */
    private boolean upsert(String adguid, String guidList, String guid, String uid) {
        boolean upserted = false;
        try {
            //update adguid to guid list mapping
            addSingleMapping(adguid, guidList, IdMapable.ADGUID_GUID_PREFIX, GUID_MAP_KEY);

            //update adguid to uid mapping
            addSingleMapping(adguid, uid, IdMapable.ADGUID_UID_PREFIX, UID_MAP_KEY);

            //update guid to adguid mapping. For reverse mapping.
            addSingleMapping(guid, adguid, IdMapable.GUID_ADGUID_PREFIX, ADGUID_MAP_KEY);

            //update guid to uid mapping
            addSingleMapping(guid, uid, IdMapable.GUID_UID_PREFIX, UID_MAP_KEY);

            //update uid to guid mapping
            addSingleMapping(uid, guid, IdMapable.UID_GUID_PREFIX, GUID_MAP_KEY);

            upserted = true;
        } catch (Exception e) {
            logger.warn("Couchbase get operation exception", e);
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","upsert"));
        }
        return upserted;
    }

    /**
     * @param key    String
     * @param val    String
     * @param expiry second
     * @return {@link boolean}
     */
    public boolean put(String key, String val, int expiry) {
        CacheClient cacheClient = null;
        try {
            cacheClient = factory.getClient(datasourceName);
            return cacheClient.set(key, expiry, val).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            MonitorUtil.info("addCBDeFaile", 1, Field.of("method","put"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return false;
    }

    public String get(String key) {
        CacheClient cacheClient = null;
        try {
            cacheClient = factory.getClient(datasourceName);
            Object o = cacheClient.get(key, StringTranscoder.getInstance());
            return o == null ? null : o.toString();
        } catch (Exception e) {
            e.printStackTrace();
            MonitorUtil.info("getCBDeFaile", 1, Field.of("method","get"));
        } finally {
            factory.returnClient(cacheClient);
        }
        return null;
    }
}
