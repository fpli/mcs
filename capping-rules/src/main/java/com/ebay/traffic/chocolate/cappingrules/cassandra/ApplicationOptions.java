package com.ebay.traffic.chocolate.cappingrules.cassandra;

import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
  }
  
  /**
   * Return the singleton
   *
   * @return singleton instance
   */
  public static ApplicationOptions getInstance() {
    return instance;
  }
}
