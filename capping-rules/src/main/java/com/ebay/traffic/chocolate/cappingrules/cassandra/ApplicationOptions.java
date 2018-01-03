package com.ebay.traffic.chocolate.cappingrules.cassandra;

import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.traffic.chocolate.cappingrules.constant.Env;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Controls the parsing of Chocolate application options.
 * <p>
 * Created by yimeng on 11/12/17.
 */
public class ApplicationOptions extends AbstractApplicationOptions {
  
  /**
   * The end point of chocolate cassandra service
   */
  public static final String CHOCO_CASSANDRA_SVC_END_POINT = "chocorprtService.chocorprtClient.endpointUri";
  
  /**
   * The end point of oauth token service
   */
  public static final String CHOCO_OAUTH_SVC_END_POINT = "chocorprtService.oauthClient.endpointUri";
  
  /**
   * Cassandra Connection Related Configurations
   */
  public static final String CHOCO_CASSANDRA_HOST = "choco.cassandra.host";
  public static final String CHOCO_CASSANDRA_PORT = "choco.cassandra.port";
  public static final String CHOCO_CASSANDRA_KEYSPACE = "choco.cassandra.keyspace";
  public static final String CHOCO_CASSANDRA_USERNAME = "choco.cassandra.username";
  public static final String CHOCO_CASSANDRA_PASSWORD = "choco.cassandra.password";
  
  public static final String SLASH = "/";
  /**
   * Private logging instance
   */
  private static final Logger logger = LoggerFactory.getLogger(ApplicationOptions.class);
  
  /**
   * Singleton instance
   */
  private static final ApplicationOptions instance = new ApplicationOptions();
  
  /**
   * For UT purposes mainly
   *
   * @param properties to initialize using
   */
  public static void init(final Properties properties) {
    instance.initInstance(properties);
  }
  
  /**
   * Application options to load from internal jar
   *
   * @param propertiesFile to load file from
   * @throws IOException if properties could not be loaded
   */
  public static void init(String propertiesFile) throws IOException {
    InputStream inputStream = ApplicationOptions.class.getClassLoader().getResourceAsStream(propertiesFile);
    Properties prop = new Properties();
    prop.load(inputStream);
    instance.initInstance(prop);
    inputStream.close();
  }
  
  /**
   * Application options to load from internal jar with Environment
   *
   * @param propertiesFile to load file from
   * @throws IOException if properties could not be loaded
   */
  public static void init(String propertiesFile, String env) throws IOException {
    if (Env.QA.name().equalsIgnoreCase(env)) {
      init(Env.QA.name().toLowerCase() + SLASH + propertiesFile);
    } else {
      init(Env.PROD.name().toLowerCase() + SLASH + propertiesFile);
    }
  }
  
  /**
   * Return the singleton
   *
   * @return singleton instance
   */
  public static ApplicationOptions getInstance() {
    return instance;
  }
  
  public String getCassandraUserName() {
    return getStringProperty(CHOCO_CASSANDRA_HOST);
  }
  
  /**
   * Get a property's value
   *
   * @param property to get
   * @return property that has string value
   * @pre no null property, no empty values
   */
  public String getStringProperty(final String property) {
    String value = getStringPropertyAllowBlank(property);
    if (StringUtils.isEmpty(value)) {
      logger.error("Configured property found, but value is null or blank! Required property:" + property);
      throw new UnsupportedOperationException(property + " is blank");
    }
    return value;
  }
  
  /**
   * Get a property's value
   *
   * @param property to get
   * @return property that has string value
   * @pre no null property, no empty values
   */
  public String getStringPropertyAllowBlank(final String property) {
    if (!properties.containsKey(property)) {
      logger.error("Configured property does not exist! Missing property:" + property);
      throw new UnsupportedOperationException(property + " not found in properties file!");
    }
    
    String value = properties.getProperty(property);
    return value;
  }
}
