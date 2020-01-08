package com.ebay.traffic.chocolate.map.dao;

import com.ebay.traffic.chocolate.map.entity.*;

import java.util.List;
import java.util.Map;

/**
 * get data count from couchbase
 */
public interface EpnMapTableCbDao {

  /**
   * get data count from AMS_TRAFFIC_SOURCE by AMS_TRAFFIC_SOURCE_ID in couchbase
   *
   * @param trafficSourceInfoList
   */
  int getTrafficSourceInfo(List<TrafficSourceInfo> trafficSourceInfoList);

  /**
   * get data count from AMS_CLICK_FILTER_TYPE by AMS_CLK_FLTR_TYPE_ID in couchbase
   *
   * @param clickFilterTypeInfoList
   */
  int getClickFilterTypeInfo(List<ClickFilterTypeInfo> clickFilterTypeInfoList);

  /**
   * get data count from AMS_PROGRAM by AMS_COUNTRY_ID in couchbase
   *
   * @param programInfoList
   */
  int getProgramInfo(List<ProgramInfo> programInfoList);

  /**
   * get data count from AMS_PROG_PUB_MAP by AMS_PROG_PUB_MAP_ID in couchbase
   *
   * @param progPubMapInfoList
   */
  int getProgPubMapInfo(List<ProgPubMapInfo> progPubMapInfoList);

  /**
   * get total count from from AMS_PROG_PUB_MAP by view in couchbase
   *
   * @param env
   */
  int getProgPubMapInfoByView(String env);

  /**
   * get data count from AMS_PUB_ADV_CLICK_FILTER_MAP by AMS_PUBLISHER_ID in couchbase
   *
   * @param clickFilterInfoMap
   */
  int getClickFilterMapInfo(Map<String, List<PubAdvClickFilterMapInfo>> clickFilterInfoMap);

  /**
   * get total count from from AMS_PUB_ADV_CLICK_FILTER_MAP by view in couchbase
   *
   * @param env
   */
  int getClickFilterMapInfoByView(String env);

  /**
   * get data count from ams_publisher_campaign by ams_publisher_campaign_id in couchbase
   *
   * @param publisherCampaignInfoList
   */
  int getPublisherCampaignInfo(List<PublisherCampaignInfo> publisherCampaignInfoList);

  /**
   * get total count from from ams_publisher_campaign by view in couchbase
   *
   * @param env
   */
  int getPublisherCampaignInfoByView(String env);

  /**
   * get data count from AMS_PUBLISHER by AMS_PUBLISHER_ID in couchbase
   *
   * @param publisherInfoList
   */
  int getPublisherInfo(List<PublisherInfo> publisherInfoList);

  /**
   * get total count from from AMS_PUBLISHER by view in couchbase
   *
   * @param env
   */
  int getPublisherInfoByView(String env);

  /**
   * get data count from AMS_PUB_DOMAIN by AMS_PUBLISHER_ID in couchbase
   *
   * @param pubDomainInfoMap
   */
  int getPubDomainInfo(Map<String, List<PubDomainInfo>> pubDomainInfoMap);

  /**
   * get total count from from AMS_PUB_DOMAIN by view in couchbase
   *
   * @param env
   */
  int getPubDomainInfoByView(String env);

}
