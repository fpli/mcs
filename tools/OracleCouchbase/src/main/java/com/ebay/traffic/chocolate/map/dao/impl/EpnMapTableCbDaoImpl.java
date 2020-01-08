package com.ebay.traffic.chocolate.map.dao.impl;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.ebay.dukes.CacheClient;
import com.ebay.traffic.chocolate.map.constant.CouchbasePrefixConstant;
import com.ebay.traffic.chocolate.map.dao.CouchbaseClient;
import com.ebay.traffic.chocolate.map.dao.EpnMapTableCbDao;
import com.ebay.traffic.chocolate.map.entity.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * get data count from couchbase
 */
@Component
public class EpnMapTableCbDaoImpl implements EpnMapTableCbDao {
  /**
   * Global logging instance
   */
  private static Logger logger = LoggerFactory.getLogger(EpnMapTableCbDaoImpl.class);

  private CouchbaseClient couchbaseClient;

  /**
   * gson instance
   */
  private Gson gson = new GsonBuilder().serializeNulls().create();

  @Autowired
  public EpnMapTableCbDaoImpl(CouchbaseClient couchbaseClient) {
    this.couchbaseClient = couchbaseClient;
  }

  /**
   * get data count from AMS_TRAFFIC_SOURCE by AMS_TRAFFIC_SOURCE_ID in couchbase
   *
   * @param trafficSourceInfoList
   */
  @Override
  public synchronized int getTrafficSourceInfo(List<TrafficSourceInfo> trafficSourceInfoList) {
    CacheClient cacheClient = null;
    Bucket bucket = null;
    int total = 0;
    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      bucket = couchbaseClient.getBucket(cacheClient);
      for (TrafficSourceInfo trafficSourceInfo : trafficSourceInfoList) {
        String trafficSourceId = CouchbasePrefixConstant.AMS_TRAFFIC_SOURCE_PREFIX + trafficSourceInfo.getAms_traffic_source_id();
        JsonDocument jsonDocument = bucket.get(trafficSourceId);
        if (jsonDocument != null) {
          total++;
        }
      }
      logger.info("TrafficSourceInfoList get finished in couchbase");
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get data count from AMS_CLICK_FILTER_TYPE by AMS_CLK_FLTR_TYPE_ID in couchbase
   *
   * @param clickFilterTypeInfoList
   */
  @Override
  public synchronized int getClickFilterTypeInfo(List<ClickFilterTypeInfo> clickFilterTypeInfoList) {
    CacheClient cacheClient = null;
    Bucket bucket = null;
    int total = 0;
    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      bucket = couchbaseClient.getBucket(cacheClient);
      for (ClickFilterTypeInfo clickFilterTypeInfo : clickFilterTypeInfoList) {
        String clickFilterTypeId = CouchbasePrefixConstant.AMS_CLICK_FILTER_TYPE_PREFIX + clickFilterTypeInfo.getAms_clk_fltr_type_id();
        JsonDocument jsonDocument = bucket.get(clickFilterTypeId);
        if (jsonDocument != null) {
          total++;
        }
      }
      logger.info("ClickFilterTypeInfoList get finished in couchbase");
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get data count from AMS_PROGRAM by AMS_COUNTRY_ID in couchbase
   *
   * @param programInfoList
   */
  @Override
  public synchronized int getProgramInfo(List<ProgramInfo> programInfoList) {
    CacheClient cacheClient = null;
    Bucket bucket = null;
    int total = 0;
    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      bucket = couchbaseClient.getBucket(cacheClient);
      for (ProgramInfo programInfo : programInfoList) {
        String proCountryId = CouchbasePrefixConstant.AMS_PROGRAM_PREFIX + programInfo.getAms_country_id();
        JsonDocument jsonDocument = bucket.get(proCountryId);
        total++;
      }

      logger.info("ProgramInfoList get finished in couchbase");
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get data count from AMS_PROG_PUB_MAP by AMS_PROG_PUB_MAP_ID in couchbase
   *
   * @param progPubMapInfoList
   */
  @Override
  public synchronized int getProgPubMapInfo(List<ProgPubMapInfo> progPubMapInfoList) {
    CacheClient cacheClient = null;
    int total = 0;
    try {
      if (!progPubMapInfoList.isEmpty()) {
        cacheClient = couchbaseClient.getEpnCacheClient();
        final Bucket bucket = couchbaseClient.getBucket(cacheClient);

        for (ProgPubMapInfo progPubMapInfo : progPubMapInfoList) {
          String progPubMapKey = CouchbasePrefixConstant.AMS_PROG_PUB_MAP_PREFIX + progPubMapInfo.getAms_publisher_id() + "_" + progPubMapInfo.getAms_program_id();
          JsonDocument jsonDocument = bucket.get(progPubMapKey);
          total++;
        }

        logger.info("progPubMapList get finished in couchbase");
      } else {
        logger.info("There is no new progPubMapList in couchbase");
      }

    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get total count from from AMS_PROG_PUB_MAP by view in couchbase
   *
   * @param env
   */
  @Override
  public int getProgPubMapInfoByView(String env) {
    CacheClient cacheClient = null;

    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      final Bucket bucket = couchbaseClient.getBucket(cacheClient);
      ViewQuery query = ViewQuery.from(env + "PPM", "PPM");
      int count = bucket.query(query).totalRows();
      logger.info("view count finished in couchbase");

      return count;
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }
  }

  /**
   * get data count from AMS_PUB_ADV_CLICK_FILTER_MAP by AMS_PUBLISHER_ID in couchbase
   *
   * @param clickFilterInfoMap
   */
  @Override
  public synchronized int getClickFilterMapInfo(Map<String, List<PubAdvClickFilterMapInfo>> clickFilterInfoMap) {
    CacheClient cacheClient = null;
    int total = 0;
    try {
      List<List<PubAdvClickFilterMapInfo>> pubAdvClickFilterInfoList = new ArrayList<>();
      if (!clickFilterInfoMap.isEmpty()) {
        for (List<PubAdvClickFilterMapInfo> pubAdvClickFilterMapInfoList : clickFilterInfoMap.values()) {
          pubAdvClickFilterInfoList.add(pubAdvClickFilterMapInfoList);
        }

        cacheClient = couchbaseClient.getEpnCacheClient();
        final Bucket bucket = couchbaseClient.getBucket(cacheClient);

        for (List<PubAdvClickFilterMapInfo> pubAdvClickFilterInfo : pubAdvClickFilterInfoList) {
          String clickFilterMapKey = CouchbasePrefixConstant.AMS_PUB_ADV_CLICK_FILTER_MAP_PREFIX + pubAdvClickFilterInfo.get(0).getAms_publisher_id();
          JsonDocument jsonDocument = bucket.get(clickFilterMapKey);
          total++;
        }

        logger.info("clickFilterMapList get finished in couchbase");
      } else {
        logger.info("There is no new clickFilterMapList in couchbase");
      }

    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get total count from from AMS_PUB_ADV_CLICK_FILTER_MAP by view in couchbase
   *
   * @param env
   */
  @Override
  public int getClickFilterMapInfoByView(String env) {
    CacheClient cacheClient = null;

    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      final Bucket bucket = couchbaseClient.getBucket(cacheClient);
      ViewQuery query = ViewQuery.from(env + "APF", "APF");
      int count = bucket.query(query).totalRows();
      logger.info("view count finished in couchbase");

      return count;
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }
  }

  /**
   * get data count from ams_publisher_campaign by ams_publisher_campaign_id in couchbase
   *
   * @param publisherCampaignInfoList
   */
  @Override
  public synchronized int getPublisherCampaignInfo(List<PublisherCampaignInfo> publisherCampaignInfoList) {
    CacheClient cacheClient = null;
    int total = 0;
    try {
      if (!publisherCampaignInfoList.isEmpty()) {
        cacheClient = couchbaseClient.getEpnCacheClient();
        final Bucket bucket = couchbaseClient.getBucket(cacheClient);

        for (PublisherCampaignInfo publisherCampaignInfo : publisherCampaignInfoList) {
          String pubCmpnKey = CouchbasePrefixConstant.AMS_PUBLISHER_CAMPAIGN_INFO_PREFIX + publisherCampaignInfo.getAms_publisher_campaign_id();
          JsonDocument jsonDocument = bucket.get(pubCmpnKey);
          total++;
        }

        logger.info("PublisherCampaignInfoList get finished in couchbase");
      } else {
        logger.info("There is no new PublisherCampaignInfoList in couchbase");
      }

    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get total count from from ams_publisher_campaign by view in couchbase
   *
   * @param env
   */
  @Override
  public int getPublisherCampaignInfoByView(String env) {
    CacheClient cacheClient = null;

    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      final Bucket bucket = couchbaseClient.getBucket(cacheClient);
      ViewQuery query = ViewQuery.from(env + "PBC", "PBC");
      int count = bucket.query(query).totalRows();
      logger.info("view count finished in couchbase");

      return count;
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }
  }

  /**
   * get data count from AMS_PUBLISHER by AMS_PUBLISHER_ID in couchbase
   *
   * @param publisherInfoList
   */
  @Override
  public synchronized int getPublisherInfo(List<PublisherInfo> publisherInfoList) {
    CacheClient cacheClient = null;
    int total = 0;
    try {
      if (!publisherInfoList.isEmpty()) {
        cacheClient = couchbaseClient.getEpnCacheClient();
        final Bucket bucket = couchbaseClient.getBucket(cacheClient);

        for (PublisherInfo publisherInfo : publisherInfoList) {
          String publisherKey = CouchbasePrefixConstant.AMS_PUBLISHER_PREFIX + publisherInfo.getAms_publisher_id();
          JsonDocument jsonDocument = bucket.get(publisherKey);
          total++;
        }

        logger.info("PublisherInfoList get finished in couchbase");
      } else {
        logger.info("There is no new PublisherInfoList in couchbase");
      }

    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get total count from from AMS_PUBLISHER by view in couchbase
   *
   * @param env
   */
  @Override
  public int getPublisherInfoByView(String env) {
    CacheClient cacheClient = null;

    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      final Bucket bucket = couchbaseClient.getBucket(cacheClient);
      ViewQuery query = ViewQuery.from(env + "PUB", "PUB");
      int count = bucket.query(query).totalRows();
      logger.info("view count finished in couchbase");

      return count;
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }
  }


  /**
   * get data count from AMS_PUB_DOMAIN by AMS_PUBLISHER_ID in couchbase
   *
   * @param pubDomainInfoMap
   */
  @Override
  public synchronized int getPubDomainInfo(Map<String, List<PubDomainInfo>> pubDomainInfoMap) {
    CacheClient cacheClient = null;
    int total = 0;
    try {
      List<List<PubDomainInfo>> pubDomainList = new ArrayList<>();
      if (!pubDomainInfoMap.isEmpty()) {
        for (List<PubDomainInfo> publisherDomainInfoList : pubDomainInfoMap.values()) {
          pubDomainList.add(publisherDomainInfoList);
        }

        cacheClient = couchbaseClient.getEpnCacheClient();
        final Bucket bucket = couchbaseClient.getBucket(cacheClient);

        for (List<PubDomainInfo> pubDomainInfos : pubDomainList) {
          String pubDomainKey = CouchbasePrefixConstant.AMS_PUB_DOMAIN_PREFIX + pubDomainInfos.get(0).getAms_publisher_id();
          JsonDocument jsonDocument = bucket.get(pubDomainKey);
          total++;
        }

        logger.info("PubDomainList get finished in couchbase");
      } else {
        logger.info("There is no new PubDomainList in couchbase");
      }

    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }

    return total;
  }

  /**
   * get total count from from AMS_PUB_DOMAIN by view in couchbase
   *
   * @param env
   */
  @Override
  public int getPubDomainInfoByView(String env) {
    CacheClient cacheClient = null;

    try {
      cacheClient = couchbaseClient.getEpnCacheClient();
      final Bucket bucket = couchbaseClient.getBucket(cacheClient);
      ViewQuery query = ViewQuery.from(env + "APD", "APD");
      int count = bucket.query(query).totalRows();
      logger.info("view count finished in couchbase");

      return count;
    } finally {
      couchbaseClient.returnCacheClient(cacheClient);
    }
  }

}
