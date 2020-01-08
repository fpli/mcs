package com.ebay.traffic.chocolate.map.dao.impl;

import com.ebay.security.nameservice.NameServiceFactory;
import com.ebay.traffic.chocolate.map.MapConfigBean;
import com.ebay.traffic.chocolate.map.dao.OracleClient;
import com.ebay.traffic.chocolate.map.provider.CredentialsProvider;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class OracleClientImpl implements OracleClient {
  private MapConfigBean configBean;
  private static Logger logger = LoggerFactory.getLogger(OracleClientImpl.class);

  /**
   * Oracle data source
   */
  private ComboPooledDataSource dataSource;

  @Autowired
  public OracleClientImpl(MapConfigBean configBean) {
    this.configBean = configBean;
  }

  @PostConstruct
  private void init() {
    try {
      dataSource = new ComboPooledDataSource();
      dataSource.setMaxPoolSize(20);
      dataSource.setMinPoolSize(5);
      dataSource.setDriverClass(configBean.getOracleDriver());
      dataSource.setJdbcUrl(configBean.getOracleUrl());
      CredentialsProvider.NamePassword np = credentialsProvider().forOracle();
      dataSource.setUser(np.name);
      dataSource.setPassword(np.password);
    } catch (Exception e) {
      logger.error("Failed to get Oracle Datasource");
    }
    logger.info("Initialize Oracle Datasource successfully.");
  }

  /**
   * For now, these are name and password for JDBC (Oracle) connection.
   *
   * @return CredentialsProvider object
   */
  public CredentialsProvider credentialsProvider() {
    return new CredentialsProvider(NameServiceFactory.getInstance());
  }

  /**
   * create oracle connection
   *
   * @return connection
   */
  public Connection getOracleConnection() throws SQLException {
    return dataSource.getConnection();
  }

  /**
   * close oracle connection
   */
  public void closeOracleConnection(Connection conn, PreparedStatement pre, ResultSet result) {
    try {
      if (result != null)
        result.close();
      if (pre != null)
        pre.close();
      if (conn != null)
        conn.close();
      logger.info("oracle connection has closed");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
