package com.ebay.traffic.chocolate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

public class ChannelActionMapUtil {

  private static final Logger logger = LoggerFactory.getLogger(ChannelActionMapUtil.class);
  private static ChannelActionMapUtil channelActionMapUtil = new ChannelActionMapUtil();
  private static String config_path = "/datashare/mkttracking/tools/AlertingAggrate-tool/conf/";
  private HashMap<String, String> channelTypeMap = new HashMap<>();
  private HashMap<String, String> actionTypeMap = new HashMap<>();

  private ChannelActionMapUtil() {

  }

  public static ChannelActionMapUtil getInstance() {
    return channelActionMapUtil;
  }

  public void init() {
    initMap("event-type.properties", channelTypeMap);
    initMap("action-type.properties", actionTypeMap);
  }

  private void initMap(String fileName, HashMap<String, String> map) {
    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(config_path + fileName));
      Properties prop = new Properties();
      prop.load(fileInputStream);

      Enumeration en = prop.propertyNames();
      while (en.hasMoreElements()) {
        String value = (String) en.nextElement();
        String key = prop.getProperty(value);
        map.put(key, value);
        logger.info(key + " : " + value);
      }

    } catch (Exception e) {
      if (fileInputStream != null) {
        try {
          fileInputStream.close();
        } catch (IOException ioException) {
          logger.info(ioException.getMessage());
        }
      }

      logger.info(e.getMessage());
    }
  }

  public String getChannelTypeById(String id) {
    return channelTypeMap.get(id);
  }

  public String getActionTypeById(String id) {
    return actionTypeMap.get(id);
  }

}
