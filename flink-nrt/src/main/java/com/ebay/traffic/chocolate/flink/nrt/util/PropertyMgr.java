package com.ebay.traffic.chocolate.flink.nrt.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyMgr {
  private PropertyEnv propertyEnv;

  public static PropertyMgr getInstance() {
    return SingletonHolder.instance;
  }

  private PropertyMgr() {
    initPropertyEnv();
  }

  private static class SingletonHolder {
    private static PropertyMgr instance = new PropertyMgr();
  }

  private void initPropertyEnv() {
    Properties prop = new Properties();
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("application.properties")) {
      prop.load(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    propertyEnv = PropertyEnv.valueOf(prop.getProperty("profiles.active").toUpperCase());
  }

  public Properties loadProperty(String propertyName) {
    Properties prop = new Properties();
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(propertyEnv.name() + "/" + propertyName)) {
      prop.load(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return prop;
  }

}
