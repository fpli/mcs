package com.ebay.traffic.chocolate.map.dao;

import com.ebay.traffic.chocolate.map.entity.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * get table data count from oracle
 */
public interface EpnMapTableOraDao {
  /**
   * Get static AMS_TRAFFIC_SOURCE table data from oracle
   */
  List<TrafficSourceInfo> getTrafficSourceInfo() throws SQLException;

  /**
   * Get static AMS_CLICK_FILTER_TYPE table data from oracle
   */
  List<ClickFilterTypeInfo> getClickFilterTypeInfo() throws SQLException;

  /**
   * Get static AMS_PROGRAM table data from oracle
   */
  List<ProgramInfo> getProgramInfo() throws SQLException;

  /**
   * Get dynamic CLICK_FILTER_ID_RULE_MAP table one day data from oracle
   */
  Map<String, List<PubAdvClickFilterMapInfo>> getClickFilterMapInfo(String startTime, String endTime) throws SQLException;

  /**
   * Get dynamic CLICK_FILTER_ID_RULE_MAP table all days data from oracle
   */
  int getClickFilterMapInfoAlldays() throws SQLException;


  /**
   * Get dynamic AMS_PROG_PUB_MAP table one day data from oracle
   */
  List<ProgPubMapInfo> getProgPubMapInfo(String startTime, String endTime) throws SQLException;

  /**
   * Get dynamic AMS_PROG_PUB_MAP table all days data from oracle
   */
  int getProgPubMapInfoAlldays() throws SQLException;

  /**
   * Get dynamic AMS_PUBLISHER_CAMPAIGN table one day data from oracle
   */
  List<PublisherCampaignInfo> getPublisherCampaignInfo(String startTime, String endTime) throws SQLException;

  /**
   * Get dynamic AMS_PUBLISHER_CAMPAIGN table all days data from oracle
   */
  int getPublisherCampaignInfoAlldays() throws SQLException;

  /**
   * Get dynamic AMS_PUBLISHER table one day data from oracle
   */
  List<PublisherInfo> getPublisherInfo(String startTime, String endTime) throws SQLException;

  /**
   * Get dynamic AMS_PUBLISHER table all days data from oracle
   */
  int getPublisherInfoAlldays() throws SQLException;

  /**
   * Get dynamic AMS_PUB_DOMAIN table one day data from oracle
   */
  Map<String, List<PubDomainInfo>> getPubDomainInfo(String startTime, String endTime) throws SQLException;

  /**
   * Get dynamic AMS_PUB_DOMAIN table all days data from oracle
   */
  int getPubDomainInfoAlldays() throws SQLException;

}
