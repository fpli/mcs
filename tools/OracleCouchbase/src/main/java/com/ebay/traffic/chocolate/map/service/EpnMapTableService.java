package com.ebay.traffic.chocolate.map.service;


import com.ebay.traffic.chocolate.map.entity.DataCountInfo;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * get data from oracle table
 */
public interface EpnMapTableService {
  /**
   * get data from oracle table AMS_TRAFFIC_SOURCE
   */
  void getTrafficSource(ArrayList<DataCountInfo> list) throws SQLException;

  /**
   * get data from oracle table AMS_CLICK_FILTER_TYPE
   */
  void getClickFilterType(ArrayList<DataCountInfo> list) throws SQLException;

  /**
   * get data from oracle table AMS_PROGRAM
   */
  void getProgramInfo(ArrayList<DataCountInfo> list) throws SQLException;

  /**
   * get data from oracle table CLICK_FILTER_ID_RULE_MAP
   */
  void getClickFilterMap(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException;

  /**
   * get data from oracle table AMS_PROG_PUB_MAP
   */
  void getProgPubMap(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException;

  /**
   * get data from oracle table AMS_PUBLISHER_CAMPAIGN
   */
  void getPublisherCampaignInfo(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException;

  /**
   * get data from oracle table AMS_PUBLISHER
   */
  void getPublisherInfo(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException;

  /**
   * get data from oracle table AMS_PUB_DOMAIN
   */
  void getPubDomainInfo(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException;

}
