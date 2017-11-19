package com.ebay.traffic.chocolate.cappingrules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by yliu29 on 11/16/17.
 *
 * HBase Connection is a heavy object and thread-safe, we can create one and share by threads.
 * Singleton object for HBase Connection.
 */
public class HBaseConnection {
  private static volatile Configuration hbaseConf;
  private static Connection connection;

  private HBaseConnection() {
  }

  /**
   * Get the Singleton object of HBase Connection.
   *
   * @return Connection The HBase Connection.
   * @throws IOException
   */
  public static Connection getConnection() throws IOException {
    if (connection == null) {
      synchronized (HBaseConnection.class) {
        if (connection == null) {
          if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create();
          }
          connection = ConnectionFactory.createConnection(hbaseConf);
        }
      }
    }
    return connection;
  }

  /**
   * Get the HBase Configuration.
   *
   * @return Configuration The HBase Configuration.
   * @throws IOException
   */
  public static Configuration getConfiguration() throws IOException {
    return getConnection().getConfiguration();
  }

  /**
   * Set HBase Configuration.
   * @param conf The HBase Configuration.
   */
  public static void setConfiguration(Configuration conf) {
    synchronized (HBaseConnection.class) {
      if (hbaseConf == null) {
        hbaseConf = conf;
      }
    }
  }
  
  /**
   * Set HBase Connection.
   * @param conc The HBase Configuration.
   */
  public static void setConnection(Connection conc) {
    synchronized (HBaseConnection.class) {
      if (connection == null) {
        connection = conc;
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
    connection.close();
  }
}
