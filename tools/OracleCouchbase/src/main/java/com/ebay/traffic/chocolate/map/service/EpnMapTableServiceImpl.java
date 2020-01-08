package com.ebay.traffic.chocolate.map.service;

import com.ebay.traffic.chocolate.map.dao.EpnMapTableCbDao;
import com.ebay.traffic.chocolate.map.dao.EpnMapTableOraDao;
import com.ebay.traffic.chocolate.map.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * get data count from oracle and couchbase
 */
@Service
public class EpnMapTableServiceImpl implements EpnMapTableService {
  /**
   * Global logging instance
   */
  private static Logger logger = LoggerFactory.getLogger(EpnMapTableServiceImpl.class);

  @Autowired
  EpnMapTableOraDao epnMapTableOraDao;

  @Autowired
  EpnMapTableCbDao epnMapTableCbDao;

  /**
   * sync oracle table AMS_TRAFFIC_SOURCE to couchbase
   */
  @Override
  public void getTrafficSource(ArrayList<DataCountInfo> list) throws SQLException {
    List<TrafficSourceInfo> trafficSourceInfoList = epnMapTableOraDao.getTrafficSourceInfo();
    int total = epnMapTableCbDao.getTrafficSourceInfo(trafficSourceInfoList);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("TrafficSource");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(trafficSourceInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setAlldayCountInCouchbase(String.valueOf(trafficSourceInfoList.size()));
    dataCountInfo.setAlldayCountInOracle(String.valueOf(total));
    dataCountInfo.setTableType("static table");

    list.add(dataCountInfo);
  }

  /**
   * sync oracle table AMS_CLICK_FILTER_TYPE to couchbase
   */
  @Override
  public void getClickFilterType(ArrayList<DataCountInfo> list) throws SQLException {
    List<ClickFilterTypeInfo> clickFilterTypeInfoList = epnMapTableOraDao.getClickFilterTypeInfo();
    int total = epnMapTableCbDao.getClickFilterTypeInfo(clickFilterTypeInfoList);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("ClickFilterType");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(clickFilterTypeInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setAlldayCountInOracle(String.valueOf(clickFilterTypeInfoList.size()));
    dataCountInfo.setAlldayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setTableType("static table");

    list.add(dataCountInfo);
  }

  /**
   * sync oracle table AMS_PROGRAM to couchbase
   */
  @Override
  public synchronized void getProgramInfo(ArrayList<DataCountInfo> list) throws SQLException {
    List<ProgramInfo> programInfoList = epnMapTableOraDao.getProgramInfo();
    int total = epnMapTableCbDao.getProgramInfo(programInfoList);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("TrafficSource");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(programInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setAlldayCountInOracle(String.valueOf(programInfoList.size()));
    dataCountInfo.setAlldayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setTableType("static table");

    list.add(dataCountInfo);
  }

  /**
   * sync oracle table CLICK_FILTER_ID_RULE_MAP to couchbase
   */
  @Override
  public synchronized void getClickFilterMap(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException {
    Map<String, List<PubAdvClickFilterMapInfo>> clickFilterMapInfoList = epnMapTableOraDao.getClickFilterMapInfo(startTime, endTime);
    int oracleTotalAlldays = epnMapTableOraDao.getClickFilterMapInfoAlldays();
    int total = epnMapTableCbDao.getClickFilterMapInfo(clickFilterMapInfoList);
    int totalAlldays = epnMapTableCbDao.getClickFilterMapInfoByView(env);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("TrafficSource");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(clickFilterMapInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setOnedayCountInOracle(String.valueOf(oracleTotalAlldays));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(totalAlldays));
    dataCountInfo.setTableType("dynamic table");

    list.add(dataCountInfo);
  }

  /**
   * sync oracle table AMS_PROG_PUB_MAP to couchbase
   */
  @Override
  public synchronized void getProgPubMap(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException {
    List<ProgPubMapInfo> progPubMapInfoList = epnMapTableOraDao.getProgPubMapInfo(startTime, endTime);
    int oracleTotalAlldays = epnMapTableOraDao.getProgPubMapInfoAlldays();
    int total = epnMapTableCbDao.getProgPubMapInfo(progPubMapInfoList);
    int totalAlldays = epnMapTableCbDao.getProgPubMapInfoByView(env);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("TrafficSource");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(progPubMapInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setOnedayCountInOracle(String.valueOf(oracleTotalAlldays));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(totalAlldays));
    dataCountInfo.setTableType("dynamic table");

    list.add(dataCountInfo);
  }

  /**
   * sync oracle table AMS_PUBLISHER_CAMPAIGN to couchbase
   */
  @Override
  public synchronized void getPublisherCampaignInfo(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException {
    List<PublisherCampaignInfo> publisherCampaignInfoList = epnMapTableOraDao.getPublisherCampaignInfo(startTime, endTime);
    int oracleTotalAlldays = epnMapTableOraDao.getPublisherCampaignInfoAlldays();
    int total = epnMapTableCbDao.getPublisherCampaignInfo(publisherCampaignInfoList);
    int totalAlldays = epnMapTableCbDao.getPublisherCampaignInfoByView(env);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("TrafficSource");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(publisherCampaignInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setOnedayCountInOracle(String.valueOf(oracleTotalAlldays));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(totalAlldays));
    dataCountInfo.setTableType("dynamic table");

    list.add(dataCountInfo);
  }

  /**
   * sync oracle table AMS_PUBLISHER to couchbase
   */
  @Override
  public synchronized void getPublisherInfo(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException {
    List<PublisherInfo> publisherInfoList = epnMapTableOraDao.getPublisherInfo(startTime, endTime);
    int oracleTotalAlldays = epnMapTableOraDao.getPublisherInfoAlldays();
    int total = epnMapTableCbDao.getPublisherInfo(publisherInfoList);
    int totalAlldays = epnMapTableCbDao.getPublisherInfoByView(env);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("TrafficSource");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(publisherInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setOnedayCountInOracle(String.valueOf(oracleTotalAlldays));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(totalAlldays));
    dataCountInfo.setTableType("dynamic table");

    list.add(dataCountInfo);
  }

  /**
   * sync oracle table AMS_PUB_DOMAIN to couchbase
   */
  @Override
  public synchronized void getPubDomainInfo(String startTime, String endTime, ArrayList<DataCountInfo> list, String env) throws SQLException {
    Map<String, List<PubDomainInfo>> pubDomainInfoList = epnMapTableOraDao.getPubDomainInfo(startTime, endTime);
    int oracleTotalAlldays = epnMapTableOraDao.getPubDomainInfoAlldays();
    int total = epnMapTableCbDao.getPubDomainInfo(pubDomainInfoList);
    int totalAlldays = epnMapTableCbDao.getPubDomainInfoByView(env);

    DataCountInfo dataCountInfo = new DataCountInfo();
    dataCountInfo.setTableName("TrafficSource");
    dataCountInfo.setOnedayCountInOracle(String.valueOf(pubDomainInfoList.size()));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(total));
    dataCountInfo.setOnedayCountInOracle(String.valueOf(oracleTotalAlldays));
    dataCountInfo.setOnedayCountInCouchbase(String.valueOf(totalAlldays));
    dataCountInfo.setTableType("dynamic table");

    list.add(dataCountInfo);
  }

  /**
   * For JUnit Testing
   *
   * @param epnMapTableCbDao
   */
  public void setCouchbaseDao(EpnMapTableCbDao epnMapTableCbDao) {
    this.epnMapTableCbDao = epnMapTableCbDao;
  }

  /**
   * For JUnit Testing
   *
   * @param epnMapTableOraDao
   */
  public void setOracleDao(EpnMapTableOraDao epnMapTableOraDao) {
    this.epnMapTableOraDao = epnMapTableOraDao;
  }
}
