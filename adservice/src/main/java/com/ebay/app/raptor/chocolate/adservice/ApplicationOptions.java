package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.kernel.context.RuntimeContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
  static final String COUCHBASE_DATASOURCE = "chocolate.adservice.couchbase.datasource";

  static final String IS_SECURE_COOKIE = "chocolate.adservice.cookie.secure";

  /**
   * Redirect default page
   */
  private static final String REDIRECT_HOMEPAGE = "chocolate.adservice.redirect.homepage";

  private static final String ATTESTATION_FILE = "privacy-sandbox-attestations.json";

  /**
   * Static driver ID
   */
  private int DRIVER_ID;

  /**
   * Application options to load from internal jar
   *
   * @throws IOException if properties could not be loaded
   */
  public static void init() throws IOException {
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

  public int setDriverId(int driverId) {
    return DRIVER_ID = driverId;
  }

  /**
   * Get couchbase datasource
   * @return datasource
   */
  public String getCouchbaseDatasource() {
    return ApplicationOptionsParser.getStringProperty(properties, COUCHBASE_DATASOURCE);
  }

  public boolean isSecureCookie() {
    return Boolean.valueOf(ApplicationOptionsParser.getStringProperty(properties, IS_SECURE_COOKIE));
  }

  public String getRedirectHomepage() {
    return ApplicationOptionsParser.getStringProperty(properties, REDIRECT_HOMEPAGE);
  }

  /**
   * Get the Privacy Sandbox attestation file
   */
  public String getAttestationFile() throws IOException {
    InputStream inputStream = null;
    String jsonTxt;
    try {
      File file = new File(RuntimeContext.getConfigRoot().getFile() + ATTESTATION_FILE);
      inputStream = new FileInputStream(file);
      jsonTxt = IOUtils.toString(inputStream, "UTF-8");
    } catch (IOException e) {
      throw e;
    } finally {
      if(inputStream != null){
        inputStream.close();
      }
    }

    return jsonTxt;
  }
}
