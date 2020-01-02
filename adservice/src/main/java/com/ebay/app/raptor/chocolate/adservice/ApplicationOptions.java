package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.kernel.context.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Controls the parsing of adservice application options.
 *
 * @author xiangli4
 */
public class ApplicationOptions extends AbstractApplicationOptions {

  /**
   * Private logging instance
   */
  private static final Logger logger = LoggerFactory.getLogger(ApplicationOptions.class);

  /**
   * Singleton instance
   */
  private static final ApplicationOptions instance = new ApplicationOptions();

  private static final String CONFIG_SUBFOLDER = "config/";

  private static final String ADSERVICE_PROPERTIES_FILE = "adservice.properties";

  /**
   * couchbase data source
   */
  public static final String COUCHBASE_DATASOURCE = "chocolate.adservice.couchbase.datasource";

  /**
   * Static driver ID
   */
  static final int DRIVER_ID = ApplicationOptionsParser.getDriverIdFromIp();

  private String outKafkaCluster;
  private Map<ChannelType, String> outKafkaConfigMap = new HashMap<>();

  /**
   * Application options to load from internal jar
   *
   * @throws IOException if properties could not be loaded
   */
  static void init() throws IOException {
    instance.initInstance(loadProperties(ADSERVICE_PROPERTIES_FILE));
  }

  private static Properties loadProperties(String file) throws IOException {
    String filePath = RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + file;
    Properties properties = new Properties();
    FileReader reader = new FileReader(filePath);
    properties.load(reader);
    reader.close();
    return properties;
  }

  /**
   * For UT purposes mainly
   *
   * @param properties to initialize using
   */
  public static void init(final Properties properties) {
    instance.initInstance(properties);
  }

  /**
   * Can't create ApplicationOptions from outside
   */
  private ApplicationOptions() {
  }

  /**
   * Return the singleton
   *
   * @return singleton instance
   */
  public static ApplicationOptions getInstance() {
    return instance;
  }

  /**
   * @return the properties for testing purposes only
   */
  public Properties getProperties() {
    return (Properties) properties.clone();
  }


  /**
   * @return the driver ID.
   */
  public int getDriverId() {
    return DRIVER_ID;
  }

  /**
   * Get couchbase datasource
   * @return datasource
   */
  public String getCouchbaseDatasource() {
    return ApplicationOptionsParser.getStringProperty(properties, COUCHBASE_DATASOURCE);
  }
}
