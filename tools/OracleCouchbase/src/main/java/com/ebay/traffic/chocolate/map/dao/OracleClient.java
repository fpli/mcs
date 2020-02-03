package com.ebay.traffic.chocolate.map.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface OracleClient {
  /**
   * create oracle connection
   *
   * @return connection
   */
  Connection getOracleConnection() throws SQLException;

  /**
   * close oracle connection
   */
  void closeOracleConnection(Connection conn, PreparedStatement pre, ResultSet result);
}
