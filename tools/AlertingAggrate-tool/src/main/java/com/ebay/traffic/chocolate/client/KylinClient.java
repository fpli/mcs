package com.ebay.traffic.chocolate.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class KylinClient {

  private static final Logger logger = LoggerFactory.getLogger(KylinClient.class);

  public static KylinClient kylinClient = new KylinClient();

  private Driver driver;
  private Properties properties;
  private Connection conn;
  private Statement state;

  public static KylinClient getInstance() {
    return kylinClient;
  }

  private KylinClient() {
    try {
      driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
      properties = new Properties();
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  public KylinClient setUserName(String userName) {
    properties.put("user", userName);
    return this;
  }

  public KylinClient setPassWord(String passWord) {
    properties.put("password", passWord);
    return this;
  }

  public KylinClient setUrl(String url) {
    try {
      conn = driver.connect(url, properties);
    } catch (SQLException e) {
      logger.info(e.getMessage());
    }
    return this;
  }

  public ResultSet getResultSet(String sql) {
    try {
      state = conn.createStatement();
      return state.executeQuery(sql);
    } catch (SQLException e) {
      logger.info(e.getMessage());
      return null;
    }
  }

}
