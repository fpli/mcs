package com.ebay.traffic.chocolate.map;

import com.ebay.kernel.bean.configuration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * A ConfigBean for properties
 */
@Component
@ConfigurationProperties(prefix = "mkttracksvc")
public class MapConfigBean extends BaseConfigBean {
  private static BeanPropertyInfo ORACLE_DRIVER = createBeanPropertyInfo("oracleDriver", "oracle_driver", true);
  private static BeanPropertyInfo ORACLE_URL = createBeanPropertyInfo("oracleUrl", "oracle_url", true);
  private static BeanPropertyInfo EPN_COUCHBASE_DATA_SOURCE = createBeanPropertyInfo("epnCbDataSource", "epn_couchbase_data_source", true);
  private static BeanPropertyInfo CP_COUCHBASE_DATA_SOURCE = createBeanPropertyInfo("cpCbDataSource", "cp_couchbase_data_source", true);


  private String oracleDriver;
  private String oracleUrl;
  /**
   * epn source file data source
   */
  private String epnCbDataSource;
  /**
   * campaign publisher mapping data source
   */
  private String cpCbDataSource;

  private static final Logger logger = LoggerFactory.getLogger(MapConfigBean.class);

  public MapConfigBean() throws ConfigCategoryCreateException {
    BeanConfigCategoryInfo categoryInfo = new BeanConfigCategoryInfoBuilder()
      .setCategoryId("com.ebay.traffic.chocolate.MapConfigBean")
      .setAlias("trackAlert")
      .setGroup("MktTrackMonitor")
      .setPersistent(true)
      .build();
    // this init method need to be called to bind the ConfigBean instance to a configuration category
    init(categoryInfo, true);
  }

  public String getOracleDriver() {
    return oracleDriver;
  }

  public void setOracleDriver(String oracleDriver) {
    changeProperty(ORACLE_DRIVER, this.oracleDriver, oracleDriver);
    logger.info("oracleDriver has been changed: " + oracleDriver);
  }

  public String getOracleUrl() {
    return oracleUrl;
  }

  public void setOracleUrl(String oracleUrl) {
    changeProperty(ORACLE_URL, this.oracleUrl, oracleUrl);
    logger.info("oracleUrl has been changed: " + oracleUrl);
  }

  public String getEpnCbDataSource() {
    return epnCbDataSource;
  }

  public void setEpnCbDataSource(String epnCbDataSource) {
    changeProperty(EPN_COUCHBASE_DATA_SOURCE, this.epnCbDataSource, epnCbDataSource);
    logger.info("EPN_COUCHBASE_DATA_SOURCE has been changed: " + epnCbDataSource);
  }

  public String getCpCbDataSource() {
    return cpCbDataSource;
  }

  public void setCpCbDataSource(String cpCbDataSource) {
    changeProperty(CP_COUCHBASE_DATA_SOURCE, this.cpCbDataSource, cpCbDataSource);
    logger.info("CP_COUCHBASE_DATA_SOURCE has been changed: " + cpCbDataSource);
  }
}


