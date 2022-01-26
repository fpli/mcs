package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class can determine the runtime environment automatically, and also provide some useful methods to get variables
 * from property files.
 *
 * @author Zhiyuan Wang
 * @since 2020/9/15
 */
public class PropertyMgr {
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyMgr.class);

  private static final String LOCAL_HOST = "localhost";
  private static final String SUFFIX_EBAY_COM = ".ebay.com";
  private static final String SUFFIX_EBAY_C3_COM = ".ebayc3.com";
  private static final String COLO_CORP = "corp";
  private static final String COLO_DEV = "dev";
  private static final String COLO_STG = "stg";

  private PropertyEnv propertyEnv;

  public static PropertyMgr getInstance() {
    return SingletonHolder.instance;
  }

  private PropertyMgr() {
    initPropertyEnv();
  }

  private static class SingletonHolder {
    private static final PropertyMgr instance = new PropertyMgr();
  }

  /**
   * Determine the runtime environment.
   * For staging, the rheos-api-endpoint should be https://rhs-streaming-api.staging.vip.ebay.com
   * For prod, the rheos-api-endpoint should be https://rhs-streaming-api.vip.ebay.com
   */
  private void initPropertyEnv() {
    String hostName;

    try {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      // can not get host name, set local by default.
      propertyEnv = PropertyEnv.DEV;
      return;
    }

    if ("rheos-streaming-prod".equals(System.getenv("MY_NAMESPACE"))) {
      propertyEnv = PropertyEnv.PROD;
      return;
    }

    String rheosApiEndpoint = GlobalConfiguration.loadConfiguration().getString(ConfigOptions
            .key(RheosConstants.RHEOS_API_ENDPOINT).stringType().defaultValue(StringConstants.EMPTY));
    if (rheosApiEndpoint.isEmpty()) {
      propertyEnv = PropertyEnv.DEV;
      return;
    }
    if (rheosApiEndpoint.endsWith("qa.ebay.com") || rheosApiEndpoint.endsWith("staging.ebay.com")) {
      propertyEnv = PropertyEnv.STAGING;
      return;
    }

    final String osName = discoverOs();
    if (LOCAL_HOST.equals(hostName)
            || hostName.toUpperCase().startsWith("D-")
            || hostName.toUpperCase().startsWith("LM-")
            || hostName.toUpperCase().startsWith("L-SHC-")
            || osName.contains("mac")
            || osName.contains("windows")) {
      propertyEnv = PropertyEnv.DEV;
      return;
    }

    // Found suffix.
    String colo = checkColo(hostName, SUFFIX_EBAY_COM);
    if (colo == null) {
      colo = checkColo(hostName, SUFFIX_EBAY_C3_COM);
    }

    // PROD VM starts with phx/slc/lvs, hostname doesn't have suffix.
    if (colo == null) {
      if (hostName.startsWith("slc")) {
        colo = "slc";
      } else if (hostName.startsWith("phx")) {
        colo = "phx";
      } else if (hostName.startsWith("lvs")) {
        colo = "lvs";
      }
    }

    if (colo == null) {
      // Still not recognized, return dev by default.
      propertyEnv = PropertyEnv.DEV;
      return;
    }

    switch (colo) {
      case COLO_CORP:
        propertyEnv = PropertyEnv.DEV;
        break;
      case COLO_DEV:
        propertyEnv = PropertyEnv.STAGING;
        break;
      case COLO_STG:
        propertyEnv = PropertyEnv.STAGING;
        break;
      // Other cases, such as 'phx', 'lvs01', 'stratus', 'vip' etc.
      default:
        propertyEnv = PropertyEnv.PROD;
    }

  }

  /**
   * Read property files from config path.
   *
   * @param propertyName property file name
   * @return property object
   */
  public Properties loadProperty(String propertyName) {
    Properties prop = new Properties();
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName)) {
      prop.load(in);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return prop;
  }

  /**
   * Read property files from config path, and split content into list
   *
   * @param propertyName property file name
   * @return file content list
   */
  public List<String> loadAllLines(String propertyName) {
    List<String> allLines = new ArrayList<>();
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName);
         BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in)))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        allLines.add(line);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return allLines;
  }

  /**
   * Read property files from config path.
   *
   * @param propertyName property file name
   * @return file content
   */
  public String loadFile(String propertyName) {
    StringJoiner joiner = new StringJoiner(StringConstants.LINE_SEPERATOR);
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName);
         BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in)))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        joiner.add(line);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return joiner.toString();
  }

  /**
   * Read property files from config path.
   *
   * @param propertyName property file name
   * @return file content
   */
  public Map<String, Object> loadYaml(String propertyName) {
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName)) {
      Yaml yaml = new Yaml();
      return yaml.load(in);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public byte[] loadBytes(String propertyName) throws IOException {
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName)) {
      return IOUtils.toByteArray(Objects.requireNonNull(in));
    }
  }

  public static String discoverOs() {
    Properties props = System.getProperties();
    return props.getProperty("os.name").toLowerCase();
  }

  private static String checkColo(String hostName, String suffix) {
    if (hostName.endsWith(suffix) && hostName.length() > suffix.length()) {
      String prePart = hostName.substring(0, hostName.length() - suffix.length());
      int idx = prePart.lastIndexOf('.') + 1;

      return prePart.substring(idx);
    }

    return null;
  }
}
