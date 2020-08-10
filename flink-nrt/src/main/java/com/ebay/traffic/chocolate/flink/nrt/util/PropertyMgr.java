package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.flink.nrt.constant.FlinkConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class PropertyMgr {
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyMgr.class);

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
    String rheosApiEndpoint = GlobalConfiguration.loadConfiguration().getString(ConfigOptions
            .key(FlinkConstants.RHEOS_API_ENDPOINT).stringType().defaultValue(StringConstants.EMPTY));
    if (rheosApiEndpoint.isEmpty()) {
      propertyEnv = PropertyEnv.DEV;
    } else if (rheosApiEndpoint.contains(PropertyEnv.STAGING.getName().toLowerCase())) {
      propertyEnv = PropertyEnv.STAGING;
    } else {
      propertyEnv = PropertyEnv.PROD;
    }
    LOGGER.info("property env is {}", propertyEnv);
  }

  public Properties loadProperty(String propertyName) {
    Properties prop = new Properties();
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName)) {
      prop.load(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return prop;
  }

  public List<String> loadAllLines(String propertyName) {
    List<String> allLines = new ArrayList<>();
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName);
         BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in)))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        allLines.add(line);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return allLines;
  }

  public String loadFile(String propertyName) {
    StringJoiner joiner = new StringJoiner(StringConstants.LINE_SEPERATOR);
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + StringConstants.SLASH + propertyName);
         BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in)))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        joiner.add(line);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return joiner.toString();
  }

}
