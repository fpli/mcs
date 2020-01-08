package com.ebay.traffic.chocolate.map.dao.impl;

import com.ebay.traffic.chocolate.map.constant.OracleColumnNameConstant;
import com.ebay.traffic.chocolate.map.constant.OracleQueryConstant;
import com.ebay.traffic.chocolate.map.dao.EpnMapTableOraDao;
import com.ebay.traffic.chocolate.map.dao.OracleClient;
import com.ebay.traffic.chocolate.map.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * get table data count from oracle
 */
@Component
public class EpnMapTableOraDaoImpl implements EpnMapTableOraDao {
  /**
   * Global logging instance
   */
  private static Logger logger = LoggerFactory.getLogger(EpnMapTableOraDaoImpl.class);

  private OracleClient oracleClient;

  @Autowired
  public EpnMapTableOraDaoImpl(OracleClient oracleClient) {
    this.oracleClient = oracleClient;
  }

  /**
   * Get static AMS_TRAFFIC_SOURCE table data from oracle
   */
  @Override
  public List<TrafficSourceInfo> getTrafficSourceInfo() throws SQLException {
    List<TrafficSourceInfo> trafficSourceInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(OracleQueryConstant.AMS_TRAFFIC_SOURCE_SQL);
      result = preparedStatement.executeQuery();
      while (result.next()) {
        TrafficSourceInfo trafficSourceInfo = new TrafficSourceInfo();
        trafficSourceInfo.setAms_traffic_source_id(result.getString(OracleColumnNameConstant.AMS_TRAFFIC_SOURCE_ID));
        trafficSourceInfo.setParent_traffic_source_id(result.getString(OracleColumnNameConstant.PARENT_TRAFFIC_SOURCE_ID));
        trafficSourceInfo.setTraffic_source_name(result.getString(OracleColumnNameConstant.TRAFFIC_SOURCE_NAME));
        trafficSourceInfo.setExposed(result.getString(OracleColumnNameConstant.EXPOSED));
        trafficSourceInfoList.add(trafficSourceInfo);
      }
      logger.info("Traffic source info get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return trafficSourceInfoList;
  }

  /**
   * Get static AMS_CLICK_FILTER_TYPE table data from oracle
   */
  @Override
  public List<ClickFilterTypeInfo> getClickFilterTypeInfo() throws SQLException {
    List<ClickFilterTypeInfo> clickFilterTypeInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(OracleQueryConstant.AMS_CLICK_FILTER_TYPE_SQL);
      result = preparedStatement.executeQuery();
      while (result.next()) {
        ClickFilterTypeInfo clickFilterTypeInfo = new ClickFilterTypeInfo();
        clickFilterTypeInfo.setAms_clk_fltr_type_id(result.getString(OracleColumnNameConstant.AMS_CLK_FLTR_TYPE_ID));
        clickFilterTypeInfo.setAms_clk_fltr_type_name(result.getString(OracleColumnNameConstant.AMS_CLK_FLTR_TYPE_NAME));
        clickFilterTypeInfo.setTraffic_source_id(result.getString(OracleColumnNameConstant.TRAFFIC_SOURCE_ID));
        clickFilterTypeInfo.setRoi_rule_id(result.getString(OracleColumnNameConstant.ROI_RULE_ID));
        clickFilterTypeInfoList.add(clickFilterTypeInfo);
      }
      logger.info("Click filter type info get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return clickFilterTypeInfoList;
  }

  /**
   * Get static AMS_PROGRAM table data from oracle
   */
  @Override
  public List<ProgramInfo> getProgramInfo() throws SQLException {
    List<ProgramInfo> programInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(OracleQueryConstant.AMS_PROGRAM_SQL);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();
      while (result.next()) {
        ProgramInfo programInfo = new ProgramInfo();
        programInfo.setAms_program_id(result.getString(OracleColumnNameConstant.AMS_PROGRAM_ID));
        programInfo.setAms_country_id(result.getString(OracleColumnNameConstant.AMS_COUNTRY_ID));
        programInfo.setProgram_name(result.getString(OracleColumnNameConstant.PROGRAM_NAME));
        programInfoList.add(programInfo);
      }
      logger.info("ProgramInfoList get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return programInfoList;

  }

  /**
   * Get dynamic CLICK_FILTER_ID_RULE_MAP table one day data from oracle
   */
  @Override
  public Map<String, List<PubAdvClickFilterMapInfo>> getClickFilterMapInfo(String startTime, String endTime) throws SQLException {
    Map<String, List<PubAdvClickFilterMapInfo>> clickFilterMapInfoMap = new HashMap<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      String resultSql = String.format(OracleQueryConstant.AMS_PUB_ADV_CLICK_FILTER_MAP_SQL, startTime, endTime);
      logger.info("Publisher click filter map info execute sql is " + resultSql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(resultSql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        String amsPublisherId = result.getString(OracleColumnNameConstant.AMS_PUBLISHER_ID);
        List<PubAdvClickFilterMapInfo> pubAdvClickFilterMapInfoList = clickFilterMapInfoMap.get(amsPublisherId);
        if (pubAdvClickFilterMapInfoList == null) {
          pubAdvClickFilterMapInfoList = new ArrayList<>();
        }
        PubAdvClickFilterMapInfo clickFilterMapInfo = generateClickFilterMapInfo(result);
        pubAdvClickFilterMapInfoList.add(clickFilterMapInfo);
        clickFilterMapInfoMap.put(amsPublisherId, pubAdvClickFilterMapInfoList);
      }
      logger.info("One day publisherIdRuleMap get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return clickFilterMapInfoMap;
  }

  /**
   * Get dynamic CLICK_FILTER_ID_RULE_MAP table all days data from oracle
   */
  @Override
  public int getClickFilterMapInfoAlldays() {
    Map<String, List<PubAdvClickFilterMapInfo>> clickFilterMapInfoMap = new HashMap<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    int count = 0;
    try {
      String resultSql = OracleQueryConstant.AMS_PUB_ADV_CLICK_FILTER_MAP_ALL_DAYS_SQL;
      logger.info("Publisher click filter map info execute sql is " + resultSql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(resultSql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();
      while (result.next()) {
        count = result.getInt(1);
      }
      logger.info("All days publisherIdRuleMap get finished from oracle");
    } catch (SQLException e) {
      logger.info(e.getMessage());
      count = 0;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return count;
  }

  /**
   * Get dynamic AMS_PROG_PUB_MAP table one day data from oracle
   */
  @Override
  public List<ProgPubMapInfo> getProgPubMapInfo(String startTime, String endTime) throws SQLException {
    List<ProgPubMapInfo> progPubMapInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      String sql = String.format(OracleQueryConstant.AMS_PROG_PUB_MAP_SQL, startTime, endTime);
      logger.info("Program publisher map info execute sql is " + sql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(sql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        ProgPubMapInfo progPubMapInfo = new ProgPubMapInfo();
        progPubMapInfo.setAms_program_id(result.getString(OracleColumnNameConstant.AMS_PROGRAM_ID));
        progPubMapInfo.setAms_publisher_id(result.getString(OracleColumnNameConstant.AMS_PUBLISHER_ID));
        progPubMapInfo.setStatus_enum(result.getString(OracleColumnNameConstant.STATUS_ENUM));
        progPubMapInfo.setReject_reason_enum(result.getString(OracleColumnNameConstant.REJECT_REASON_ENUM));
        progPubMapInfoList.add(progPubMapInfo);
      }
      logger.info("One day programPublisherMapList get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return progPubMapInfoList;
  }

  /**
   * Get dynamic AMS_PROG_PUB_MAP table all days data from oracle
   */
  @Override
  public int getProgPubMapInfoAlldays() {
    List<ProgPubMapInfo> progPubMapInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    int count = 0;
    try {
      String sql = OracleQueryConstant.AMS_PROG_PUB_MAP_ALL_DAYS_SQL;
      logger.info("Program publisher map info execute sql is " + sql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(sql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();
      while (result.next()) {
        count = result.getInt(1);
      }
      logger.info("All days ProgramPublisherMapList get finished from oracle");
    } catch (SQLException e) {
      logger.info(e.getMessage());
      count = 0;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return count;
  }

  /**
   * Get dynamic AMS_PUBLISHER_CAMPAIGN table one day data from oracle
   */
  @Override
  public List<PublisherCampaignInfo> getPublisherCampaignInfo(String startTime, String endTime) throws SQLException {
    List<PublisherCampaignInfo> publisherCampaignInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      String sql = String.format(OracleQueryConstant.AMS_PUBLISHER_CAMPAIGN_INFO_SQL, startTime, endTime);
      logger.info("Publisher campaign info execute sql is " + sql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(sql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        PublisherCampaignInfo publisherCampaignInfo = new PublisherCampaignInfo();
        publisherCampaignInfo.setAms_publisher_campaign_id(result.getString(OracleColumnNameConstant.AMS_PUBLISHER_CAMPAIGN_ID));
        publisherCampaignInfo.setAms_publisher_id(result.getString(OracleColumnNameConstant.AMS_PUBLISHER_ID));
        publisherCampaignInfo.setPublisher_campaign_name(result.getString(OracleColumnNameConstant.PUBLISHER_CAMPAIGN_NAME));
        publisherCampaignInfo.setStatus_enum(result.getString(OracleColumnNameConstant.STATUS_ENUM));
        publisherCampaignInfoList.add(publisherCampaignInfo);
      }
      logger.info("One day publisherCampaignInfoList get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return publisherCampaignInfoList;
  }

  /**
   * Get dynamic AMS_PUBLISHER_CAMPAIGN table all days data from oracle
   */
  @Override
  public int getPublisherCampaignInfoAlldays() {
    List<PublisherCampaignInfo> publisherCampaignInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    int count = 0;
    try {
      String sql = OracleQueryConstant.AMS_PUBLISHER_CAMPAIGN_INFO_ALL_DAYS_SQL;
      logger.info("Publisher campaign info execute sql is " + sql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(sql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        count = result.getInt(1);
      }
      logger.info("All days publisherCampaignInfoList get finished from oracle");
    } catch (SQLException e) {
      logger.info(e.getMessage());
      count = 0;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return count;
  }

  /**
   * Get dynamic AMS_PUBLISHER table one day data from oracle
   */
  @Override
  public List<PublisherInfo> getPublisherInfo(String startTime, String endTime) throws SQLException {
    List<PublisherInfo> publisherInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      String sql = String.format(OracleQueryConstant.AMS_PUBLISHER_SQL, startTime, endTime);
      logger.info("Publisher info execute sql is " + sql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(sql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        PublisherInfo publisherInfo = new PublisherInfo();
        publisherInfo.setAms_publisher_id(result.getString(OracleColumnNameConstant.AMS_PUBLISHER_ID));
        publisherInfo.setApplication_status_enum(result.getString(OracleColumnNameConstant.APPLICATION_STATUS_ENUM));
        publisherInfo.setBiztype_enum(result.getString(OracleColumnNameConstant.BIZTYPE_ENUM));
        publisherInfo.setAms_publisher_bizmodel_id(result.getString(OracleColumnNameConstant.AMS_PUBLISHER_BIZMODEL_ID));
        publisherInfo.setAms_currency_id(result.getString(OracleColumnNameConstant.AMS_CURRENCY_ID));
        publisherInfo.setAms_country_id(result.getString(OracleColumnNameConstant.AMS_COUNTRY_ID));
        publisherInfoList.add(publisherInfo);
      }
      logger.info("One day publisherInfoList get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return publisherInfoList;
  }

  /**
   * Get dynamic AMS_PUBLISHER table all days data from oracle
   */
  @Override
  public int getPublisherInfoAlldays() {
    List<PublisherInfo> publisherInfoList = new ArrayList<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    int count = 0;
    try {
      String sql = String.format(OracleQueryConstant.AMS_PUBLISHER_ALL_DAYS_SQL);
      logger.info("Publisher info execute sql is " + sql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(sql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        count = result.getInt(1);
      }
      logger.info("All days publisherInfoList get finished from oracle");
    } catch (SQLException e) {
      logger.info(e.getMessage());
      count = 0;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return count;
  }

  /**
   * Get dynamic AMS_PUB_DOMAIN table one day data from oracle
   */
  @Override
  public Map<String, List<PubDomainInfo>> getPubDomainInfo(String startTime, String endTime) throws SQLException {
    Map<String, List<PubDomainInfo>> pubDomainInfoMap = new HashMap<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    try {
      String resultSql = String.format(OracleQueryConstant.AMS_PUB_DOMAIN_SQL, startTime, endTime);
      logger.info("Publisher domain map info execute sql is " + resultSql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(resultSql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        String amsPublisherId = result.getString(OracleColumnNameConstant.AMS_PUBLISHER_ID);
        List<PubDomainInfo> pubDomainMapInfoList = pubDomainInfoMap.get(amsPublisherId);
        if (pubDomainMapInfoList == null) {
          pubDomainMapInfoList = new ArrayList<>();
        }
        PubDomainInfo pubDomainInfo = generatePubDomainInfo(result);
        pubDomainMapInfoList.add(pubDomainInfo);
        pubDomainInfoMap.put(amsPublisherId, pubDomainMapInfoList);
      }
      logger.info("One day publisherDomainInfo get finished from oracle");
    } catch (SQLException e) {
      throw e;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return pubDomainInfoMap;
  }

  /**
   * Get dynamic AMS_PUB_DOMAIN table all days data from oracle
   */
  @Override
  public int getPubDomainInfoAlldays() {
    Map<String, List<PubDomainInfo>> pubDomainInfoMap = new HashMap<>();
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;
    int count = 0;
    try {
      String resultSql = OracleQueryConstant.AMS_PUB_DOMAIN_ALL_DAYS_SQL;
      logger.info("Publisher domain map info execute sql is " + resultSql);

      connection = oracleClient.getOracleConnection();
      connection.setAutoCommit(false);
      preparedStatement = connection.prepareStatement(resultSql);
      preparedStatement.setFetchSize(2000);
      result = preparedStatement.executeQuery();

      while (result.next()) {
        count = result.getInt(1);
      }
      logger.info("All days publisherDomainInfo get finished from oracle");
    } catch (SQLException e) {
      logger.info(e.getMessage());
      count = 0;
    } finally {
      oracleClient.closeOracleConnection(connection, preparedStatement, result);
    }
    return count;
  }


  /**
   * get ClickFilterMapInfo when get CLICK_FILTER_ID_RULE_MAP table from oracle
   */
  public PubAdvClickFilterMapInfo generateClickFilterMapInfo(ResultSet result) throws SQLException {
    PubAdvClickFilterMapInfo clickFilterMapInfo = new PubAdvClickFilterMapInfo();
    clickFilterMapInfo.setAms_pub_adv_clk_fltr_map_id(result.getString(OracleColumnNameConstant.AMS_PUB_ADV_CLK_FLTR_MAP_ID));
    clickFilterMapInfo.setAms_publisher_id(result.getString(OracleColumnNameConstant.AMS_PUBLISHER_ID));
    clickFilterMapInfo.setAms_advertiser_id(result.getString(OracleColumnNameConstant.AMS_ADVERTISER_ID));
    clickFilterMapInfo.setAms_clk_fltr_type_id(result.getString(OracleColumnNameConstant.AMS_CLK_FLTR_TYPE_ID));
    clickFilterMapInfo.setStatus_enum(result.getString(OracleColumnNameConstant.STATUS_ENUM));
    return clickFilterMapInfo;
  }

  /**
   * get PubDomainInfo when get AMS_PUB_DOMAIN table from oracle
   */
  public PubDomainInfo generatePubDomainInfo(ResultSet result) throws SQLException {
    PubDomainInfo pubDomainInfo = new PubDomainInfo();
    pubDomainInfo.setAms_pub_domain_id(result.getString(OracleColumnNameConstant.AMS_PUB_DOMAIN_ID));
    pubDomainInfo.setAms_publisher_id(result.getString(OracleColumnNameConstant.AMS_PUBLISHER_ID));
    pubDomainInfo.setValidation_status_enum(result.getString(OracleColumnNameConstant.VALIDATION_STATUS_ENUM));
    pubDomainInfo.setUrl_domain(result.getString(OracleColumnNameConstant.URL_DOMAIN));
    pubDomainInfo.setDomain_type_enum(result.getString(OracleColumnNameConstant.DOMAIN_TYPE_ENUM));
    pubDomainInfo.setWhitelist_status_enum(result.getString(OracleColumnNameConstant.WHITELIST_STATUS_ENUM));
    pubDomainInfo.setDomain_status_enum(result.getString(OracleColumnNameConstant.DOMAIN_STATUS_ENUM));
    pubDomainInfo.setFull_url(result.getString(OracleColumnNameConstant.FULL_URL));
    pubDomainInfo.setPublisher_bizmodel_id(result.getString(OracleColumnNameConstant.PUBLISHER_BIZMODEL_ID));
    pubDomainInfo.setPublisher_category_id(result.getString(OracleColumnNameConstant.PUBLISHER_CATEGORY_ID));
    pubDomainInfo.setIs_registered(result.getString(OracleColumnNameConstant.IS_REGISTERED));
    return pubDomainInfo;
  }


}
