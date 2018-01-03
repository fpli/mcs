package com.ebay.traffic.chocolate.cappingrules.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;

import java.io.IOException;

/**
 * Created by yimeng on 12/28/17.
 * <p>
 * Cassandra Connection is a heavy object and thread-safe, we can create one and share by threads.
 * Singleton object for Cassandra Connection.
 */
public class CassandraClient {
  
  private static final String CASSANDRA_PROPERTY_FILE = "cassandra.properties";
  private static Cluster cluster;
  private static Session session;
  private static MappingManager mappingManager;
  private static CassandraClient clientInstance = null;
  
  private CassandraClient() {
  }
  
  private static void init(String env) throws IOException {
    if (clientInstance != null) {
      return;
    }
    clientInstance = new CassandraClient();
    session = clientInstance.getSession(env);
    mappingManager = clientInstance.getMappingManager();
  }
  
  public static CassandraClient getInstance(String env) throws IOException {
    if (clientInstance == null) {
      init(env);
    }
    return clientInstance;
  }
  
  public Session getSession(String env) throws IOException {
    if (session == null) {
      if (session == null) {
        cluster = getCluster(env);
        ApplicationOptions applicationOptions = ApplicationOptions.getInstance();
        session = cluster.connect(applicationOptions.getStringProperty(ApplicationOptions.CHOCO_CASSANDRA_KEYSPACE));
      }
    }
    return session;
  }
  
  public Cluster getCluster(String env) throws IOException {
    if (session == null) {
      if (cluster == null) {
        ApplicationOptions.init(CASSANDRA_PROPERTY_FILE, env);
        ApplicationOptions applicationOptions = ApplicationOptions.getInstance();
        
        Cluster.Builder b = Cluster.builder()
            .addContactPoints(applicationOptions.getStringProperty(ApplicationOptions.CHOCO_CASSANDRA_HOST).split(","))
            .withPort(Integer.parseInt(applicationOptions.getStringProperty(ApplicationOptions.CHOCO_CASSANDRA_PORT)))
            .withCredentials(
                applicationOptions.getStringPropertyAllowBlank(ApplicationOptions.CHOCO_CASSANDRA_USERNAME),
                applicationOptions.getStringPropertyAllowBlank(ApplicationOptions.CHOCO_CASSANDRA_PASSWORD));
        cluster = b.build();
      }
    }
    return cluster;
  }
  
  /**
   * Applications should use this method to get wrapped mapping manager which logs events to CAL
   * and delegates the actual work to real Mapping manager.
   */
  public MappingManager getMappingManager() throws IOException {
    if (mappingManager == null) {
      mappingManager = new MappingManager(session);
    }
    return mappingManager;
  }
  
  @Override
  protected void finalize() throws Throwable {
    if (session != null) {
      session.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }
  
  public void closeClient() {
    if (clientInstance != null) {
      if (session != null) {
        session.close();
      }
      
      if (cluster != null) {
        cluster.close();
      }
      mappingManager = null;
      clientInstance = null;
      cluster = null;
      session = null;
    }
  }
}
