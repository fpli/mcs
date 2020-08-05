package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class PropertyMgr {
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

  private void initPropertyEnv() {
    Properties prop = new Properties();
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(PropertyConstants.APPLICATION_PROPERTIES)) {
      prop.load(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    propertyEnv = PropertyEnv.valueOf(prop.getProperty(PropertyConstants.PROFILES_ACTIVE).toUpperCase());
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
